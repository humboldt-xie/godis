package main
import (
	"github.com/godis/leveldb"
	"fmt"
	"time"
)

func write(db *leveldb.LevelDB,key string){
	i:=1
	for true{
		db.Put(key,"value:"+fmt.Sprintf("%d",i));
		i+=1
	}
}
func read(db *leveldb.LevelDB,key string){
	for true{
		v,err:=db.Get(key);
		fmt.Printf("%s %s\n",v,err)
		time.Sleep(1*time.Second)	
	}
}

func main() {
	db,err:=leveldb.OpenLevelDB("db");
	go write(db,"hello1");
	go write(db,"hello2")
	err=db.Put("hello","value");
	err=db.Put("hello1","value2");
	v,err:=db.Get("hello");
	fmt.Printf("%s %s\n",v,err);
	v,err=db.Get("hello1");
	fmt.Printf("%s %s\n",v,err);

	time.Sleep(10 * time.Second)

	iter:=db.Iter()
	iter.Seek("hello");
	for iter.Valid() {
		fmt.Printf("%s %s\n",iter.Key(),iter.Value())
		iter.Next()
	}
	defer iter.Destroy();
}
