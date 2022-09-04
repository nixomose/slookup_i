// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package main

import (
	"bytes"
	"fmt"
	"math/rand"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_entry"
	"github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_src"

	"time"
)

type slookup_i_test_lib struct {
	log *tools.Nixomosetools_logger
}

func New_slookup_i_test_lib(log *tools.Nixomosetools_logger) slookup_i_test_lib {

	var slookup_test slookup_i_test_lib
	slookup_test.log = log
	return slookup_test

}

//    private String get_random_data_string(int len)
//      {
//        /* return a string of random length of random characters from 1 to len characters. */
//        String ret = new String();
//
//        Random rand = new Random();
//        int count = rand.nextInt(len - 1) + 1;
//        for (int lp = 0; lp < count; lp++)
//          {
//            char c = (char)(rand.nextInt(26) + 'a');
//            ret += c;
//          }
//        return ret;
//      }

func Padstringto4k(in string) []byte {
	var out []byte //= make([]byte, 0)
	var inb = []byte(in)
	for len(out) < 4096 {
		out = append(out, inb...)
	}
	out = out[0:4096]
	return out
}

func padto4k(in []byte) []byte {
	var out []byte //= make([]byte, 0)
	for len(out) < 4096 {
		out = append(out, in...)
	}
	out = out[0:4096]
	return out
}

// func Key_from_block_num(block_num uint64) string {

// 	// this is the size of the key that the thing storing the key makes
// 	/* in order for the test to work while the test is using the real aligned file backing store
// 	it has to use the same actual keys (and more importantly lengths) that the aligned store does. */
// 	var key string = zosbd2_slookup_i_storage_mechanism.Generate_key_from_block_num(block_num)
// 	return key
// }

func binstringstart(start int) []byte {
	var out []byte = make([]byte, 256)
	for i := 0; i < 256; i++ {
		out[i] = byte((i + start) % 256)
	}
	return out
}

func (this *slookup_i_test_lib) Slookup_4k_tests(s *slookup_i_src.Slookup_i) tools.Ret {

	//	/ var m map[uint64][]byte = make(map[uint64][]byte)
	for i := 0; i < 10000; i++ {

		var k0 = rand.Uint32() % 11
		var d0 = rand.Uint64() % 11
		var data0 []byte = padto4k(binstringstart(int(d0)))
		this.log.Debug("\n\nupdating block: ", k0)

		var ret tools.Ret
		var entry *slookup_i_lib.Slookup_i_entry
		if ret, entry = s.Lookup_entry_load(k0); ret != nil {
			return ret
		}

		entry.Set_value(&data0)
		if ret = s.Data_block_store(entry); ret != nil {
			return ret
		}
		s.Diag_dump(entry)

		this.log.Debug("\n\nreading back block: ", k0)

		var entry_back *slookup_i_lib.Slookup_i_entry
		if ret, entry_back = s.Lookup_entry_load(k0); ret != nil {
			return ret
		}

		var dback *[]byte
		if ret, dback = s.Data_block_load(entry_back); ret != nil {
			return ret
		}
		if res := bytes.Compare(data0, *dback); res != 0 {
			this.log.Error("\n\ndata after store and load doesn't match: ", k0)
			panic(2)
		}

	}

	var data0 []byte = padto4k(binstringstart(0))
	var data1 []byte = padto4k(binstringstart(1))
	var data2 []byte = padto4k(binstringstart(2))
	var data3 []byte = padto4k(binstringstart(3))
	var data4 []byte = padto4k(binstringstart(4))

	var data16 []byte = padto4k(binstringstart(16))
	var data17 []byte = padto4k(binstringstart(17))
	var data18 []byte = padto4k(binstringstart(18))
	var data19 []byte = padto4k(binstringstart(19))
	var data20 []byte = padto4k(binstringstart(20))

	var k0 = rand.Uint64() % 100
	var k1 = rand.Uint64() % 100
	var k2 = rand.Uint64() % 100
	var k3 = rand.Uint64() % 100
	var k4 = rand.Uint64() % 100

	this.log.Debug("\n\ninserting data ", k0)
	s.Update_or_insert(Key_from_block_num(k0), data0)
	s.Diag_dump(false)

	this.log.Debug("\n\ninserting data ", k1)
	s.Update_or_insert(Key_from_block_num(k1), data1)
	s.Diag_dump(false)

	this.log.Debug("\n\ninserting data ", k2)
	s.Update_or_insert(Key_from_block_num(k2), data2)
	s.Diag_dump(false)

	this.log.Debug("\n\ninserting data ", k3)
	s.Update_or_insert(Key_from_block_num(k3), data3)
	s.Diag_dump(false)

	this.log.Debug("\n\ninserting data ", k4)
	s.Update_or_insert(Key_from_block_num(k4), data4)
	s.Diag_dump(true)

	this.log.Debug("\n\nupdating data0 with data16")
	s.Update_or_insert(Key_from_block_num(k0), data16)
	s.Diag_dump(true)

	this.log.Debug("\n\nupdating data1 with data17")
	s.Update_or_insert(Key_from_block_num(k1), data17)
	s.Diag_dump(true)

	this.log.Debug("\n\nupdating data2 with data18")
	s.Update_or_insert(Key_from_block_num(k2), data18)
	s.Diag_dump(true)

	this.log.Debug("\n\nupdating data3 with data19")
	s.Update_or_insert(Key_from_block_num(k3), data19)
	s.Diag_dump(true)

	this.log.Debug("\n\nupdating data4 with data20")
	s.Update_or_insert(Key_from_block_num(k4), data20)
	s.Diag_dump(true)

}

func (this *slookup_i_test_lib) get_data_string(k uint32, keylen uint32) string {
	/* return a string of random length from 1 to len characters, with the key repeated. */
	rand.Seed(time.Now().UnixNano())

	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	var b []rune = make([]rune, keylen)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func newByteableString(s string) string {
	return s
}

func newByteableByteArray(s string) []byte {
	return []byte(s)
}

func newByteableInt(d int) []byte {
	var r = fmt.Sprintf("%d", d)
	return []byte(r)
}

func (this *slookup_i_test_lib) slookup_test_bigdata(s *slookup_i_lib.slookup_i, KEY_LENGTH uint32, VALUE_LENGTH uint32) {
	/* add a node, delete a node. */
	s.Insert(newByteableString("aa"), newByteableByteArray("a234b234c234d234"))
	s.Insert(newByteableString("bb"), newByteableByteArray("e234f234g234h234"))
	s.Print(this.log)
	s.Delete(newByteableString("bb"), true)
	s.Print(this.log)
	s.Delete(newByteableString("aa"), true)
	s.Print(this.log)

	s.Insert(newByteableString("aa"), newByteableByteArray("a234b234c234d234"))
	s.Insert(newByteableString("bb"), newByteableByteArray("e234f234g234h234"))
	s.Print(this.log)
	s.Delete(newByteableString("aa"), true)
	s.Print(this.log)
	s.Delete(newByteableString("bb"), true)
	s.Print(this.log)

}

func (this *slookup_i_test_lib) slookup_test_offspring(s *slookup_i_lib.slookup_i, KEY_LENGTH uint32, VALUE_LENGTH uint32, nodes_per_block uint32) {

	// ArrayList<Boolean> foundresp = new ArrayList();
	// ArrayList<byte[]> resp = new ArrayList();

	/* going to try and make a setup where updating to a smaller node causes the mother node to be moved.
	 * basically we want o1 o2 o3 o4 m1 and then shrink the node so it moves m1 */

	s.Insert(newByteableString("aa"), newByteableByteArray("abcdefghijklmnopqrst"))
	var ret, foundresp, resp = s.Fetch(newByteableString("aa"))
	this.log.Debug(resp)
	s.Print(this.log)

	s.Insert(newByteableString("bb"), newByteableByteArray("ABCDEFGHIJKLMNOPQRST"))
	_, _, resp = s.Fetch(newByteableString("bb"))
	this.log.Debug(resp)
	s.Print(this.log)

	// that gets me a1a2a3a4a5b1b2b3b4b5
	s.Update_or_insert(newByteableString("aa"), newByteableByteArray("1234"))
	s.Print(this.log)
	_, _, resp = s.Fetch(newByteableString("aa"))
	this.log.Debug(resp)
	s.Print(this.log)

	// that should get me a1b2b3b4b5b1
	// now we shrink bb and it should exercise all the weird stuff.
	s.Update_or_insert(newByteableString("bb"), newByteableByteArray("ZYXW"))
	_, _, resp = s.Fetch(newByteableString("bb"))
	this.log.Debug(resp)
	s.Print(this.log)

	// this should fail
	ret = s.Insert(newByteableString("MX"), newByteableByteArray("biggerthanmax45678901"))
	if ret == nil {
		this.log.Error("this should have failed.")
	}
	s.Print(this.log)

	s.Insert(newByteableString("10"), newByteableByteArray("a"))
	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// size to two nodes
	s.Update_or_insert(newByteableString("10"), newByteableByteArray("abcdefg"))
	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// size back to one node
	s.Update_or_insert(newByteableString("10"), newByteableByteArray("z"))
	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// size to three nodes
	s.Update_or_insert(newByteableString("10"), newByteableByteArray("abcdefghij"))
	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// size back to two nodes
	s.Update_or_insert(newByteableString("10"), newByteableByteArray("stuvwx"))
	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// add more to aa
	s.Update_or_insert(newByteableString("aa"), newByteableByteArray("s1s2s3s4s5s6s7s8s9"))
	_, _, resp = s.Fetch(newByteableString("aa"))
	this.log.Debug(resp)
	s.Print(this.log)

	// now delete aa
	s.Delete(newByteableString("aa"), true)
	s.Print(this.log)

	/* don't change any of the tests above here, at this point we have a really good test
	    * case, there are two items, bb and 10. bb is one node, 10 is two nodes and the second
	    * node is in the first spot.
	    * so when you delete 10, the first thing it does is  delete the offspring which is in the first
	    * spot, by moving 10's mother node into it, which causes it to rewrite the relations of 10's
	    * mother node, but that node has already been logically deleted and doesn't really exist.
	    * so it should just be moved and no pointers changed.
	   10 bb
	   -- (wx) bb (ZYXW) 10 (stuv)
	   root node: 2 free position: 4
	*/
	// make sure bb and 10 still are correct
	_, _, resp = s.Fetch(newByteableString("bb"))
	this.log.Debug(resp)
	s.Print(this.log)

	_, _, resp = s.Fetch(newByteableString("10"))
	this.log.Debug(resp)
	s.Print(this.log)

	// now delete 10
	s.Delete(newByteableString("10"), true)
	s.Print(this.log)

	/* now lets add delete and update lots of stuff... */

	//        for (lp = 0; lp < 100; lp++)
	//          {
	//            int k = (int)(Math.random() * 30);
	//            String key = new String("0" + k);
	//            key = key.substring(key.length() - 2);
	//            ByteableString keystr = newByteableString(key);
	//            error("deleting key: " + key);
	//            s.delete(keystr);
	//            treeprinter_iii.printNode(s, s.get_root_node());
	//            s.Print(lib.log)
	//          }

	/* now do a bunch of random inserts updates and deletes */

	for lp := 0; lp < 1000; lp++ {
		s.Diag_dump(false)
		this.log.Debug(
			"---------------------------------------starting new run----------------------------------------------")
		for rp := 0; rp < 150; rp++ {
			var k uint32 = uint32((rand.Intn(99)))
			var key string = string("0") + tools.Uint32tostring(k)
			key = key[:len(key)-2]
			var value string = this.get_data_string(k, VALUE_LENGTH*nodes_per_block)
			this.log.Debug("update or insert key: " + key + " with " + value)
			var keystr = newByteableString(key)
			var valuestr = newByteableByteArray(value)
			if s.Update_or_insert(keystr, valuestr) != nil {
				break
			}
			//                treeprinter_iii.printNode(s, s.get_root_node());
			s.Print(this.log)
		}
		// now delete until there are only 5 left
		for s.Get_free_position() > 5 {
			var k int = rand.Intn(99)
			var key string = string("0") + tools.Inttostring(k)
			key = key[:len(key)-2]
			var keystr string = newByteableString(key)
			ret, foundresp, _ = s.Fetch(keystr)
			if ret != nil {
				break
			}
			var b bool = foundresp
			if b == false {
				continue
			}
			this.log.Debug("deleting existing key: " + key)
			if s.Delete(keystr, true) != nil {
				break
			}
			//                treeprinter_iii.printNode(s, s.get_root_node());
			s.Print(this.log)
		}
	}

	this.log.Debug("---------------------------------------ending run----------------------------------------------")

}

func (this *slookup_i_test_lib) slookup_test_run(s *slookup_i_lib.slookup_i, KEY_LENGTH uint32, VALUE_LENGTH uint32) {

	var tp slookup_i_lib.Treeprinter_iii

	s.Insert(newByteableString("10"), newByteableInt(10))
	s.Print(this.log)
	// tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("05"), newByteableInt(5))
	s.Print(this.log)
	// tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("15"), newByteableInt(15))
	s.Print(this.log)
	// tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("02"), newByteableInt(2))
	s.Print(this.log)
	// tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("08"), newByteableInt(8))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("12"), newByteableInt(12))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("18"), newByteableInt(18))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("01"), newByteableInt(1))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("04"), newByteableInt(4))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("06"), newByteableInt(6))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("14"), newByteableInt(14))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("16"), newByteableInt(16))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("19"), newByteableInt(19))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())
	s.Insert(newByteableString("09"), newByteableInt(9))
	s.Print(this.log)
	tp.PrintNode(s, s.Get_root_node())

	this.log.Error("delete root node")

	for s.Get_root_node() != 0 {
		var n *slookup_i_node.slookup_node = s.Load(s.Get_root_node())
		s.Get_logger().Debug("before delete of " + n.Get_key())
		if s.Delete(n.Get_key(), true) != nil {
			break
		}
		s.Get_logger().Debug("after delete of " + n.Get_key())
		tp.PrintNode(s, s.Get_root_node())
		s.Print(this.log)
	}

	var lp int
	for lp = 0; lp < 100; lp++ {
		var k int = (int)(rand.Intn(30) % 30)
		var key string = tools.Inttostring(0) + tools.Inttostring(k)
		key = key[len(key)-2:]
		s.Get_logger().Debug("update or insert key: " + key)
		var keystr string = newByteableString(key)
		var valueint []byte = newByteableInt(lp)
		if s.Update_or_insert(keystr, valueint) != nil {
			break
		}
		tp.PrintNode(s, s.Get_root_node())
		s.Print(this.log)
	}

	for lp = 0; lp < 100; lp++ {
		var k int = rand.Intn(30) % 30
		var key string = tools.Inttostring(0) + tools.Inttostring(k)
		key = key[len(key)-2:]
		var keystr string = newByteableString(key)

		var ret, foundresp, _ = s.Fetch(keystr)
		if ret != nil {
			break
		}
		var b bool = foundresp
		if b == false {
			continue
		}
		this.log.Debug("deleting existing key: " + key)
		if s.Delete(keystr, true) != nil {
			break
		}
		tp.PrintNode(s, s.Get_root_node())
		s.Print(this.log)
	}

	/* now do a bunch of random inserts and deletes */

	for lp = 0; lp < 100; lp++ {
		for rp := 0; rp < 40; rp++ {
			var k int = rand.Intn(30) % 30
			var key string = tools.Inttostring(0) + tools.Inttostring(k)

			key = key[:len(key)-2] //                key = key.substring(key.length() - 2);
			this.log.Debug("update or insert key: " + key)
			var keystr string = newByteableString(key)
			var valueint []byte = newByteableInt(lp)
			var ret = s.Update_or_insert(keystr, valueint)
			if ret != nil {
				break
			}

			tp.PrintNode(s, s.Get_root_node())
			s.Print(this.log)
		}
		// now delete until there are only 5 left
		for s.Get_free_position() > 5 {
			var k int = (rand.Intn(30) % 30)
			var key string = tools.Inttostring(0) + tools.Inttostring(k)
			key = key[:len(key)-2]
			var keystr string = newByteableString(key)

			var ret, foundresp, _ = s.Fetch(keystr)
			if ret != nil {
				break
			}
			var b bool = foundresp
			if b == false {
				continue
			}
			this.log.Debug("deleting existing key: " + key)

			if s.Delete(keystr, true) != nil {
				break
			}
			// treeprinter_iii.printNode(s, s.get_root_node());
			s.Print(this.log)
		}
	}
}
