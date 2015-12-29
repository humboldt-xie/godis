package main
import (
	"github.com/humboldt_xie/godis/leveldb"
	"bufio"
	"flag"
	"fmt"
	"net"
	redis "github.com/humboldt_xie/godis/redis"
)

var g_port int
func init () {
	flag.IntVar(&g_port,"p",6389,"port")
}

type MyGodis struct{
	db *leveldb.LevelDB
}

func NewMyGodis(path string) *MyGodis {
	db,_:=leveldb.OpenLevelDB(path);
	return &MyGodis{db};
}

func (h *MyGodis) GET(key string) ([]byte, error) {
    v,err:=h.db.Get(key);
	if err != nil {
		return nil,err
	}
    return []byte(v), nil
}

func (h *MyGodis) SET(key string, value []byte) error {
	return h.db.Put(key,string(value));
}

func (h *MyGodis ) AreYouMaster(key string) (string,error) {
	return "yes",nil;
}

func (h *MyGodis ) AddMember(host string,value []byte) error{
	fmt.Printf("add member:%s %s\n",host,value);
	go h.dump(host,string(value),0)
	return nil;
}

func (h *MyGodis) Monitor() (*redis.MonitorReply, error) {
	return &redis.MonitorReply{}, nil
}

func (h *MyGodis) dump(host string,port string,index int){
	conn,_:=net.Dial("tcp",host+":"+port)
	bulk:=redis.MultiBulkFromMap(map[string]interface{}{"sync": []byte(fmt.Sprintf("%d",index))})
	bulk.WriteTo(conn)
	r := bufio.NewReader(conn)
	for {
		request, err := redis.ParseRequest(conn,r)
		if err != nil {
			fmt.Printf("error %s \n",err)
			return 
		}
		if request.Name=="put" {
			key,_:=request.GetString(0)
			value,_:=request.GetString(1)
			fmt.Printf("%s %s\n",key,value)
			h.db.Put(key,value)
		}
	}
	return 

}

func (h *MyGodis)Sync(channel string, channels ...[]byte) (*redis.ChannelWriter, error){
	output := make(chan []interface{})
	writer := &redis.ChannelWriter{
		FirstReply: []interface{}{
			[]byte("sync"), // []byte
			[]byte(channel),             // string
			[]byte("1"),                   // int
		},
		Channel: output,
	}
	go func() {
		iter:=h.db.Iter();
		defer iter.Destroy();
		iter.SeekToFirst()
		for iter.Valid() {
			output <- []interface{}{
				[]byte("put"),
				[]byte(iter.Key()),
				[]byte(iter.Value()),
			}
			iter.Next()
		}
		close(output)
	}()
	return writer, nil

}


func main() {
	flag.Parse()
	mygodis:=NewMyGodis("db-"+fmt.Sprintf("%d",g_port));
	c:=redis.DefaultConfig()
	c.Handler(mygodis)
	c.Port(g_port)
    server,_ := redis.NewServer(c)
    server.ListenAndServe()
}
