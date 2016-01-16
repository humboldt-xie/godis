package main

import (
	"flag"
	"fmt"
	"github.com/humboldt-xie/godis/leveldb"
	redis "github.com/humboldt-xie/redisio"
	"log"
	"net"
	"strings"
)

var g_port int
var g_host string

func init() {
	flag.IntVar(&g_port, "p", 6389, "port")
	flag.StringVar(&g_host, "h", "127.0.0.1", "host")
}

type HandlerFn func(c net.Conn, rc *redis.Conn, r *redis.Package) error

type Server struct {
	methods map[string]HandlerFn
	db      *leveldb.LevelDB
	Channel chan *redis.Package
}

func (srv *Server) Register(name string, fn HandlerFn) {
	if srv.methods == nil {
		srv.methods = make(map[string]HandlerFn)
	}
	if fn != nil {
		srv.methods[strings.ToLower(name)] = fn
	}
}

func (srv *Server) ServeClient(c net.Conn) error {
	defer c.Close()
	rc := redis.NewConn(c, c)
	for {
		p, err := rc.NewPackage()
		if err != nil {
			return err
		}
		cmd := p.GetLower(0)
		fn, exists := srv.methods[cmd]
		if !exists {
			_, err := rc.WriteError("ERR unknown command '" + cmd + "'")
			return err
		}
		err = fn(c, rc, p)
		if err != nil {
			return err
		}
	}
}
func (srv *Server) CopyData(host string, port string) {
	conn, _ := net.Dial("tcp", host+":"+port)
	rc := redis.NewConn(conn, conn)
	rc.WriteMultiBytes([]interface{}{"sync", "0"})
	for {
		p, err := rc.NewPackage()
		if err != nil {
			log.Printf("Error:%s\n", err)
			return
		}
		//log.Printf("copy:%s %s\n", p.GetString(0), p.GetString(1))
		if p.GetLower(0) == "put" || p.GetLower(0) == "set" {
			key := p.GetString(1)
			value := p.GetString(2)
			srv.db.Put(key, value)
		}
	}
}

func (srv *Server) ListenAndServe(addr string) error {
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return e
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go srv.ServeClient(conn)
	}
}

func main() {
	flag.Parse()
	fmt.Printf("listen %s:%d\n", g_host, g_port)
	var svc Server
	db, _ := leveldb.OpenLevelDB("db-" + fmt.Sprintf("%d", g_port))
	svc.db = db
	svc.Channel = make(chan *redis.Package, 1000)

	svc.Register("SET", func(c net.Conn, rc *redis.Conn, r *redis.Package) error {
		if r.Count() < 3 {
			_, err := rc.WriteError("ERR wrong number of arguments for 'set' command")
			return err

		}
		key := r.GetString(1)
		value := r.GetString(2)
		err := db.Put(key, string(value))
		if err != nil {
			_, err := rc.WriteError("FAILED")
			return err
		}
		svc.Channel <- r
		_, err = rc.WriteLine("OK")
		return err
	})

	svc.Register("GET", func(c net.Conn, rc *redis.Conn, r *redis.Package) error {
		if r.Count() < 2 {
			_, err := rc.WriteError("ERR wrong number of arguments for 'put' command")
			return err

		}
		key := r.GetString(1)
		v, err := db.Get(key)
		if err != nil {
			_, err := rc.WriteError("FAILED")
			return err
		}
		_, err = rc.WriteBytes(v)
		return err
	})
	svc.Register("slaveof", func(c net.Conn, rc *redis.Conn, r *redis.Package) error {
		if r.Count() < 3 {
			_, err := rc.WriteError("ERR wrong number of arguments for 'slaveof' command")
			return err
		}
		go svc.CopyData(r.GetString(1), r.GetString(2))
		_, err := rc.WriteLine("OK")
		return err
	})
	svc.Register("sync", func(c net.Conn, rc *redis.Conn, r *redis.Package) error {
		iter := db.Iter()
		defer iter.Destroy()
		iter.SeekToFirst()
		i := 0
		for ; iter.Valid(); i++ {
			v := []interface{}{"put", string(iter.Key()), string(iter.Value())}
			_, err := rc.WriteMultiBytes(v)
			if err != nil {
				return err
			}
			select {
			case r := <-svc.Channel:
				if r.GetString(1) < iter.Key() {
					_, err := rc.WriteMultiBytes(r.Data)
					if err != nil {
						return err
					}
				}
				break
			default:
			}
			iter.Next()
		}
		fmt.Printf("copy end count:%d start sync:\n", i)
		for {
			r := <-svc.Channel
			_, err := rc.WriteMultiBytes(r.Data)
			if err != nil {
				return err
			}
		}
		return nil
	})
	e := svc.ListenAndServe(fmt.Sprintf("%s:%d", g_host, g_port))
	fmt.Printf("Listen Error %s\n", e)

	//mygodis:=node.NewDataServer("db-"+fmt.Sprintf("%d",g_port),g_host,g_port);
	//c:=redis.DefaultConfig()
	//c.Handler(mygodis)
	//c.Port(g_port)
	//c.Host(g_host)
	//server,err := redis.NewServer(c)
	//if server==nil {
	//	fmt.Printf("%s\n",err)
	//	return
	//}
	//server.ListenAndServe()
}
