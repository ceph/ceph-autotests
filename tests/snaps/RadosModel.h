// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "rados/librados.hpp"

#include <iostream>
#include <sstream>
#include <map>
#include <set>
#include <list>
#include <string>
#include <stdlib.h>
#include <pthread.h>

using namespace std;

/* Snap creation/removal tester
 */

struct RadosTestContext;

template <typename T>
typename T::iterator rand_choose(T &cont)
{
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) retval++;
  return retval;
}

struct TestOp
{
  librados::Rados::AioCompletion *completion;
  bool done;
  virtual void begin() = 0;

  virtual void finalize()
  {
    return;
  }

  virtual bool finished()
  {
    return true;
  }
};


struct TestOpGenerator
{
  virtual TestOp *next(RadosTestContext &context) = 0;
};

struct RadosTestContext
{
  pthread_mutex_t state_lock;
  pthread_cond_t wait_cond;
  map<int, map<string,string> > pool_obj_cont;
  set<int> snaps;
  set<string> oid_in_use;
  set<string> oid_not_in_use;
  int current_snap;
  string pool_name;
  librados::pool_t pool;
  librados::Rados rados;
  int next_oid;
  string prefix;
  int errors;
  int max_in_flight;
	
  RadosTestContext(const string &pool_name, int max_in_flight) :
    pool_obj_cont(),
    current_snap(0),
    pool_name(pool_name),
    errors(0),
    max_in_flight(max_in_flight)
  {
		pthread_mutex_init(&state_lock, 0);
		pthread_cond_init(&wait_cond, 0);
    rados.initialize(0, 0);
    rados.open_pool(pool_name.c_str(), &pool);
    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
  }

  void shutdown()
  {
		pthread_cond_destroy(&wait_cond);
		pthread_mutex_destroy(&state_lock);
    rados.shutdown();
  }

  void loop(TestOpGenerator &gen)
  {
    list<TestOp*> inflight;
    lock();

    TestOp *next = gen.next(*this);
    while (next || inflight.size()) {
      if (next) {
	inflight.push_back(next);
      }
      unlock();
      if (next) {
	(*inflight.rbegin())->begin();
      }
      lock();
      while (1) {
	for (list<TestOp*>::iterator i = inflight.begin();
	     i != inflight.end();) {
	  if ((*i)->finished()) {
	    delete *i;
	    inflight.erase(i++);
	  } else {
	    ++i;
	  }
	}
				
	if (inflight.size() >= (unsigned) max_in_flight || (!next && inflight.size())) {
	  cout << "Waiting on " << inflight.size() << std::endl;
	  wait();
	} else {
	  break;
	}
      }
      next = gen.next(*this);
    }
    unlock();
  }

  void wait()
  {
		pthread_cond_wait(&wait_cond, &state_lock);
  }

  void kick()
  {
		pthread_cond_signal(&wait_cond);
  }

	void lock()
	{
		pthread_mutex_lock(&state_lock);
	}
	
	void unlock()
	{
		pthread_mutex_unlock(&state_lock);
	}

  bool find_object(string oid, string &contents, int snap = -1) const
  {
    for (map<int, map<string,string> >::const_reverse_iterator i = 
	   pool_obj_cont.rbegin();
	 i != pool_obj_cont.rend();
	 ++i) {
      if (snap != -1 && snap < i->first) continue;
      if (i->second.count(oid) != 0) {
	contents = i->second.find(oid)->second;
	return true;
      }
    }
    return false;
  }

  void remove_snap(int snap)
  {
    map<int, map<string,string> >::iterator next_iter = pool_obj_cont.find(snap);
    map<int, map<string,string> >::iterator current_iter = next_iter++;
    if (next_iter != pool_obj_cont.end()) {
      map<string,string> &current = current_iter->second;
      map<string,string> &next = next_iter->second;
      for (map<string,string>::iterator i = current.begin();
	   i != current.end();
	   ++i) {
	if (next.count(i->first) == 0) {
	  next[i->first] = i->second;
	}
      }
    }
    snaps.erase(snap);
    pool_obj_cont.erase(current_iter);
  }

  void add_snap()
  {
    current_snap++;
    pool_obj_cont[current_snap];
    snaps.insert(current_snap - 1);
  }

  void roll_back(string oid, int snap)
  {
    string contents;
    find_object(oid, contents, snap);
    pool_obj_cont.rbegin()->second[oid] = contents;
  }
};

void callback(librados::completion_t cb, void *arg) {
  TestOp *op = (TestOp *) arg;
  op->finalize();
}

struct WriteOp : public TestOp
{
  RadosTestContext &context;
  string oid;
  string written;
  WriteOp(RadosTestContext &cont, const string &oid) : 
    context(cont),
    oid(oid)
  {}
		
  void begin()
  {
    context.lock();
    done = 0;
    completion = context.rados.aio_create_completion((void *) this, &callback, 0);
    stringstream to_write;
    to_write << context.prefix << "OID: " << oid << " snap " << context.current_snap << std::endl;
    written = to_write.str();
    context.pool_obj_cont[context.current_snap][oid] = written;

    context.oid_in_use.insert(oid);
    if (context.oid_not_in_use.count(oid) != 0) {
      context.oid_not_in_use.erase(oid);
    }

    librados::bufferlist write_buffer;
    write_buffer.append(to_write.str());
    context.unlock();

    context.rados.aio_write(context.pool,
			    context.prefix+oid,
			    0,
			    write_buffer,
			    to_write.str().length(),
			    completion);
  }

  void finalize()
  {
    context.lock();
    context.oid_in_use.erase(oid);
    context.oid_not_in_use.insert(oid);
    context.kick();
    done = true;
    context.unlock();
  }

  bool finished()
  {
    return done && completion->is_complete();
  }
};

struct ReadOp : public TestOp
{
  RadosTestContext &context;
  string oid;
  librados::bufferlist result;
  string old_value;
  ReadOp(RadosTestContext &cont, const string &oid) : 
    context(cont),
    oid(oid)
  {}
		
  void begin()
  {
    context.lock();
    done = 0;
    completion = context.rados.aio_create_completion((void *) this, &callback, 0);

    context.oid_in_use.insert(oid);
    context.oid_not_in_use.erase(oid);
    context.find_object(oid, old_value);

    context.unlock();
    context.rados.aio_read(context.pool,
			   context.prefix+oid,
			   0,
			   &result,
			   old_value.length(),
			   completion);
  }

  void finalize()
  {
    context.lock();
    context.oid_in_use.erase(oid);
    context.oid_not_in_use.insert(oid);
    string to_check;
    result.copy(0, old_value.length(), to_check);
    if (to_check != old_value) {
      context.errors++;
      cerr << "Error: oid " << oid << " read returned \n"
	   << to_check << "\nShould have returned\n"
	   << old_value << "\nCurrent snap is " << context.current_snap << std::endl;
    }
    context.kick();
    done = true;
    context.unlock();
  }

  bool finished()
  {
    return done && completion->is_complete();
  }
};

struct SnapCreateOp : public TestOp
{
  RadosTestContext &context;
  SnapCreateOp(RadosTestContext &cont) :
    context(cont)
  {}

  void begin()
  {
    context.lock();
    context.add_snap();
    context.unlock();

    stringstream snap_name;
    snap_name << context.prefix << context.current_snap - 1;
    context.rados.snap_create(context.pool,
			      snap_name.str().c_str());
  }
};

struct SnapRemoveOp : public TestOp
{
  RadosTestContext &context;
  int to_remove;
  SnapRemoveOp(RadosTestContext &cont, int snap) :
    context(cont),
    to_remove(snap)
  {}

  void begin()
  {
    context.lock();
    context.remove_snap(to_remove);
    context.unlock();

    stringstream snap_name;
    snap_name << context.prefix << to_remove;
    context.rados.snap_remove(context.pool,
			      snap_name.str().c_str());
  }
};

struct RollbackOp : public TestOp
{
  RadosTestContext &context;
  string oid;
  int roll_back_to;

  RollbackOp(RadosTestContext &cont, const string &_oid, int snap) :
    context(cont),
    oid(_oid),
    roll_back_to(snap)
  {}

  void begin()
  {
    context.lock();
    context.oid_in_use.insert(oid);
    context.roll_back(oid, roll_back_to);
    context.unlock();
	
    stringstream snap_name;
    snap_name << context.prefix << roll_back_to;
    context.rados.snap_rollback_object(context.pool,
				       context.prefix+oid,
				       snap_name.str().c_str());
  }
};
