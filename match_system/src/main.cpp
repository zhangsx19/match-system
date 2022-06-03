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
#include <unistd.h>

#include "save_client/Save.h"
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/server/TThreadedServer.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace  ::match_service;
using namespace  ::save_service;

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

class MatchCloneFactory : virtual public MatchIfFactory {
    public:
        ~MatchCloneFactory() override = default;
        MatchIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) override
        {
            std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(connInfo.transport);
            /*
               cout << "Incoming connection\n";//来的transport的IP
               cout << "\tSocketInfo: "  << sock->getSocketInfo() << "\n";
               cout << "\tPeerHost: "    << sock->getPeerHost() << "\n";
               cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
               cout << "\tPeerPort: "    << sock->getPeerPort() << "\n";
               */
            return new MatchHandler;
        }
        void releaseHandler(MatchIf* handler) override {
            delete handler;
        }
};

class Pool
{
    public:
        void add(User user){
            users.push_back(user);
            wt.push_back(0);//新进来的初始化等待时间为0
        }
        void remove(User user){
            for(uint32_t i = 0;i<users.size();i++){
                if(users[i].id == user.id){
                    users.erase(users.begin()+i);
                    wt.erase(wt.begin()+i);
                    break;
                }
            }
        }
        bool check_match(uint32_t i,uint32_t j){
            auto a = users[i],b = users[j];//注意a，b要能互相匹配
            int ds = abs(a.score-b.score);
            int a_max_dif = wt[i]*50,b_max_dif = wt[j]*50;
            //cout<<"ds:"<<ds<<" "<<"a:"<<a_max_dif<<" "<<"b:"<<b_max_dif<<endl;
            return ds<=a_max_dif&&ds<=b_max_dif;

        }
        void match(){
            for(uint32_t i = 0;i<wt.size();i++){
                wt[i]++;
            }
            while(users.size()>1){
                bool flag = true;
                for(uint32_t i=0;i<users.size();i++){
                    for(uint32_t j=i+1;j<users.size();j++){
                        if(check_match(i,j)){
                            auto a = users[i],b = users[j];
                            users.erase(users.begin()+j);//注意先删后面那个
                            wt.erase(wt.begin()+j);
                            users.erase(users.begin()+i);
                            wt.erase(wt.begin()+i);
                            save_result(a.id,b.id);
                            flag = false;
                            break;
                        }
                    }
                    if(!flag)break;//只需匹配一次
                }
                if(flag)break;

            }
        }
        void save_result(int a,int b){//传入两个id
            printf("Match Result: %d %d\n",a,b);

            std::shared_ptr<TTransport> socket(new TSocket("123.57.47.211", 9090));
            std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
            SaveClient client(protocol);

            try {
                transport->open();

                int res = client.save_data("acs_5622","2b365dc9",a,b);
                if(!res) puts("success!");
                else puts("failed");

                transport->close();
            } catch (TException& tx) {
                cout << "ERROR: " << tx.what() << endl;
            }
        }
    private:
        vector<User> users;
        vector<int> wt;//等待时间，单位秒
}pool;


void consume_task(){
    while(true){
        unique_lock<mutex> lck(message_queue.m);//循环完一次会解锁再获得锁
        if(message_queue.q.empty()){
            //message_queue.cv.wait(lck);
            lck.unlock();
            pool.match();
            sleep(1);//每秒匹配1次
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
        }
    }
}
int main(int argc, char **argv) {
    TThreadedServer server(
            std::make_shared<MatchProcessorFactory>(std::make_shared<MatchCloneFactory>()),
            std::make_shared<TServerSocket>(9090), //port
            std::make_shared<TBufferedTransportFactory>(),
            std::make_shared<TBinaryProtocolFactory>());
    cout<<"start match server"<<endl;

    thread matching_thread(consume_task);//死循环，单独分配线程

    server.serve();
    return 0;
}

