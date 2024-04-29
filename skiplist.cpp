#include"skiplist.h"
#include<ctime>
SkipList::Node::Node(uint64_t key,const std::string &s):
        key(key),s(s)
{
    for(int i=0;i<MAX_LEVEL;++i)
        forward[i]=nullptr;
}
unsigned int SkipList::random_level()
{
    return (*random_int)(random_engine);
}
SkipList::SkipList()
{
    head=new Node(0,"");
    tail=new Node(0,"");
    for(int i=0;i<MAX_LEVEL;++i)
        head->forward[i]=tail->forward[i]=tail;
    sz=0;
    random_engine.seed(time(nullptr));
    random_int=new std::uniform_int_distribution<unsigned int>(0,MAX_LEVEL-1);
}
SkipList::~SkipList()
{
    Node *ncur=head;
    while(ncur!=tail)
    {
        Node *ntmp=ncur;
        ncur=ncur->forward[0];
        delete ntmp;
    }
    delete ncur;
}
unsigned int SkipList::size() const
{
    return sz;
}
void SkipList::put(uint64_t key,const std::string &s)
{
    Node *ncur=head;
    Node *back[8];
    for(int i=MAX_LEVEL-1;i>=0;--i)
    {
        while(ncur->forward[i]!=tail&&ncur->forward[i]->key<key)
            ncur=ncur->forward[i];
        back[i]=ncur;
    }
    if(ncur->forward[0]!=tail&&ncur->forward[0]->key==key)
    {
        ncur->forward[0]->s=s;
        return ;
    }
    Node *ntmp=new Node(key,s);
    for(int i=0;i<MAX_LEVEL;++i)
        ntmp->forward[i]=tail;
    for(int i=random_level();i>=0;--i)
    {
        ntmp->forward[i]=back[i]->forward[i];
        back[i]->forward[i]=ntmp;
    }
    sz++;
}
std::string SkipList::get(uint64_t key) const
{
    Node *ncur=head;
    for(int i=MAX_LEVEL-1;i>=0;--i)
        while(ncur->forward[i]!=tail&&ncur->forward[i]->key<key)
            ncur=ncur->forward[i];
    if(ncur->forward[0]!=tail&&ncur->forward[0]->key==key)
        return ncur->forward[0]->s;
    return "";
}
bool SkipList::del(uint64_t key)
{
    Node *ncur=head;
    for(int i=MAX_LEVEL-1;i>=0;--i)
    {
        while(ncur->forward[i]!=tail&&ncur->forward[i]->key<key)
            ncur=ncur->forward[i];
        if(ncur->forward[i]!=tail&&ncur->forward[i]->key==key)
        {
            Node *ntmp=ncur->forward[i];
            ncur->forward[i]=ntmp->forward[i];
            for(int j=i-1;j>=0;--j)
            {
                while(ncur->forward[j]!=tail&&ncur->forward[j]->key<key)
                    ncur=ncur->forward[j];
                ncur->forward[j]=ntmp->forward[j];
            }
            delete ntmp;
            sz--;
            return true;
        }
    }
    return false;
}
void SkipList::reset()
{
    Node *ncur=head->forward[0];
    while(ncur!=tail)
    {
        Node *ntmp=ncur;
        ncur=ncur->forward[0];
        delete ntmp;
    }
    for(int i=0;i<MAX_LEVEL;++i)
        head->forward[i]=tail;
    sz=0;
}
void SkipList::scan(uint64_t key1,uint64_t key2,std::list<std::pair<uint64_t,std::string>> &list) const
{
    if(head->forward[0]==tail||head->forward[0]->key>key2)
        return ;
    Node *n1=head;
    for(int i=MAX_LEVEL-1;i>=0;--i)
        while(n1->forward[i]!=tail&&n1->forward[i]->key<key1)
            n1=n1->forward[i];
    if(n1->forward[0]==tail) return ;
    n1=n1->forward[0];
    Node *n2=n1;
    for(int i=MAX_LEVEL-1;i>=0;--i)
        while(n2->forward[i]!=tail&&n2->forward[i]->key<=key2)
            n2=n2->forward[i];
    n2=n2->forward[0];
    while(n1!=n2)
    {
        list.push_back(std::make_pair(n1->key,n1->s));
        n1=n1->forward[0];
    }
}