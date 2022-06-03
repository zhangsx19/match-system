#include "match_server/Match.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace  ::match_service;
using namespace std;

struct Task
{
    User user;
    string type;
};
struct MessageQueue
{
    queue<Task> q;
    mutex m;
    condition_variable cv;
}message_queue;

class MatchHandler : virtual public MatchIf {
 public:
  MatchHandler() {
    // Your initialization goes here
  }

  /**
   * user: 添加的用户信息
   * info: 附加信息
   * 在匹配池中添加一个名用户
   * 
   * @param user
   * @param info
   */
  int32_t add_user(const User& user, const std::string& info) {
    // Your implementation goes here
    printf("add_user\n");
    unique_lock<mutex> lck(message_queue.m);//无需显式解锁，局部变量注销即解锁
    message_queue.q.push({user,"add"});
    message_queue.cv.notify_all();
    return 0;
  }

  /**
   * user: 删除的用户信息
   * info: 附加信息
   * 从匹配池中删除一名用户
   * 
   * @param user
   * @param info
   */
  int32_t remove_user(const User& user, const std::string& info) {
    // Your implementation goes here
    printf("remove_user\n");
    unique_lock<mutex> lck(message_queue.m);//无需显式解锁，局部变量注销即解锁
    message_queue.q.push({user,"remove"});
    message_queue.cv.notify_all();
    return 0;
  }

};
class Pool
{
public:
    void add(User user){
        users.push_back(user);
    }
    void remove(User user){
        for(uint32_t i = 0;i<users.size();i++){
            if(users[i].id == user.id){
                users.erase(users.begin()+i);
                break;
            }
        }
    }
    void match(){
        while(users.size()>1){
            auto a = users[0],b = users[1];
            save_result(a.id,b.id);
            users.erase(users.begin()+1);
            users.erase(users.begin());
        }
    }
    void save_result(int a,int b){//传入两个id
        printf("Match Result: %d %d\n",a,b);
    }
private:
    vector<User> users;
}pool;
void consume_task(){
    while(true){
        unique_lock<mutex> lck(message_queue.m);//循环完一次会解锁再获得锁
        if(message_queue.q.empty()){
            message_queue.cv.wait(lck);
        }
        else{
            auto task = message_queue.q.front();
            message_queue.q.pop();
            lck.unlock();
            //deal with task
            if(task.type == "add"){
                pool.add(task.user);
            }
            else if(task.type == "remove"){
                pool.remove(task.user);
            }
            pool.match();
        }
    }
}
int main(int argc, char **argv) {
  int port = 9090;
  ::std::shared_ptr<MatchHandler> handler(new MatchHandler());
  ::std::shared_ptr<TProcessor> processor(new MatchProcessor(handler));
  ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  cout<<"start match server"<<endl;

  thread matching_thread(consume_task);//死循环，单独分配线程

  server.serve();
  return 0;
}

