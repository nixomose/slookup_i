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

	var k0 = rand.Uint32() % 100
	var k1 = rand.Uint32() % 100
	var k2 = rand.Uint32() % 100
	var k3 = rand.Uint32() % 100
	var k4 = rand.Uint32() % 100

	this.log.Debug("\n\ninserting data ", k0)
	if ret := s.Write(k0, &data0); ret != nil {
		return ret
	}
	s.Diag_dump_block(k0)

	this.log.Debug("\n\ninserting data ", k1)
	if ret := s.Write(k1, &data1); ret != nil {
		return ret
	}
	s.Diag_dump_block(k1)

	this.log.Debug("\n\ninserting data ", k2)
	if ret := s.Write(k2, &data2); ret != nil {
		return ret
	}
	s.Diag_dump_block(k2)

	this.log.Debug("\n\ninserting data ", k3)
	if ret := s.Write(k3, &data3); ret != nil {
		return ret
	}
	s.Diag_dump_block(k3)

	this.log.Debug("\n\ninserting data ", k4)
	if ret := s.Write(k4, &data4); ret != nil {
		return ret
	}
	s.Diag_dump_block(k4)

	this.log.Debug("\n\nupdating data0 with data16")
	if ret := s.Write(k0, &data16); ret != nil {
		return ret
	}
	s.Diag_dump_block(k0)

	this.log.Debug("\n\nupdating data1 with data17")
	if ret := s.Write(k1, &data17); ret != nil {
		return ret
	}
	s.Diag_dump_block(k1)

	this.log.Debug("\n\nupdating data2 with data18")
	if ret := s.Write(k2, &data18); ret != nil {
		return ret
	}
	s.Diag_dump_block(k2)

	this.log.Debug("\n\nupdating data3 with data19")
	if ret := s.Write(k3, &data19); ret != nil {
		return ret
	}
	s.Diag_dump_block(k3)

	this.log.Debug("\n\nupdating data4 with data20")
	if ret := s.Write(k4, &data20); ret != nil {
		return ret
	}
	s.Diag_dump_block(k4)
	return nil
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

func newByteableInt(d int) *[]byte {
	var r = fmt.Sprintf("%d", d)
	var ret = []byte(r)
	return &ret
}

func (this *slookup_i_test_lib) Slookup_test_writing_zero(s *slookup_i_src.Slookup_i) tools.Ret {
	/* write a block then clear it so it unmarks/deletes all of the child nodes */

	var data0 []byte = padto4k(binstringstart(0))
	var dataempty []byte = make([]byte, 0)

	var k0 = rand.Uint32() % 100

	this.log.Debug("\n\ninserting data block ", k0)
	if ret := s.Write(k0, &data0); ret != nil {
		return ret
	}
	s.Diag_dump_block(k0)

	this.log.Debug("\n\nclearning data block ", k0)
	if ret := s.Write(k0, &dataempty); ret != nil {
		return ret
	}
	s.Diag_dump_block(k0)

	return nil
}

// func (this *slookup_i_test_lib) slookup_test_offspring(s *slookup_i_src.Slookup_i, KEY_LENGTH uint32,
// 	VALUE_LENGTH uint32, nodes_per_block uint32) {

// 	// now delete 10
// 	if ret := s.Delete(newByteableString("10"), true); ret != nil {
// 		return ret
// 	}
// 	s.Print(this.log)

// 	/* now lets add delete and update lots of stuff... */

// 	//        for (lp = 0; lp < 100; lp++)
// 	//          {
// 	//            int k = (int)(Math.random() * 30);
// 	//            String key = new String("0" + k);
// 	//            key = key.substring(key.length() - 2);
// 	//            ByteableString keystr = newByteableString(key);
// 	//            error("deleting key: " + key);
// 	//            s.delete(keystr);
// 	//            treeprinter_iii.printNode(s, s.get_root_node());
// 	//            s.Print(lib.log)
// 	//          }

// 	/* now do a bunch of random inserts updates and deletes */

// 	for lp := 0; lp < 1000; lp++ {
// 		s.Diag_dump(false)
// 		this.log.Debug(
// 			"---------------------------------------starting new run----------------------------------------------")
// 		for rp := 0; rp < 150; rp++ {
// 			var k uint32 = uint32((rand.Intn(99)))
// 			var key string = string("0") + tools.Uint32tostring(k)
// 			key = key[:len(key)-2]
// 			var value string = this.get_data_string(k, VALUE_LENGTH*nodes_per_block)
// 			this.log.Debug("update or insert key: " + key + " with " + value)
// 			var keystr = newByteableString(key)
// 			var valuestr = newByteableByteArray(value)
// 			if s.Update_or_insert(keystr, valuestr) != nil {
// 				break
// 			}
// 			//                treeprinter_iii.printNode(s, s.get_root_node());
// 			s.Print(this.log)
// 		}
// 		// now delete until there are only 5 left
// 		for s.Get_free_position() > 5 {
// 			var k int = rand.Intn(99)
// 			var key string = string("0") + tools.Inttostring(k)
// 			key = key[:len(key)-2]
// 			var keystr string = newByteableString(key)
// 			ret, foundresp, _ = s.Fetch(keystr)
// 			if ret != nil {
// 				break
// 			}
// 			var b bool = foundresp
// 			if b == false {
// 				continue
// 			}
// 			this.log.Debug("deleting existing key: " + key)
// 			if s.Delete(keystr, true) != nil {
// 				break
// 			}
// 			//                treeprinter_iii.printNode(s, s.get_root_node());
// 			s.Print(this.log)
// 		}
// 	}

// 	this.log.Debug("---------------------------------------ending run----------------------------------------------")

// }

func (this *slookup_i_test_lib) slookup_test_run(s *slookup_i_src.Slookup_i, uhh uint32,
	KEY_LENGTH uint32, VALUE_LENGTH uint32) tools.Ret {

	if ret := s.Write(10, newByteableInt(10)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(05, newByteableInt(5)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(15, newByteableInt(15)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(02, newByteableInt(2)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(8, newByteableInt(8)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(12, newByteableInt(12)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(18, newByteableInt(18)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(01, newByteableInt(1)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(04, newByteableInt(4)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(06, newByteableInt(6)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(14, newByteableInt(14)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(16, newByteableInt(16)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(19, newByteableInt(19)); ret != nil {
		return ret
	}
	s.Print(this.log)

	if ret := s.Write(9, newByteableInt(9)); ret != nil {
		return ret
	}
	s.Print(this.log)

	// this.log.Error("delete root node")

	// for s.Get_root_node() != 0 {
	// 	var n *slookup_i_node.slookup_node = s.Load(s.Get_root_node())
	// 	s.Get_logger().Debug("before delete of " + n.Get_key())
	// 	if s.Delete(n.Get_key(), true) != nil {
	// 		break
	// 	}
	// 	s.Get_logger().Debug("after delete of " + n.Get_key())
	// 	tp.PrintNode(s, s.Get_root_node())
	// 	s.Print(this.log)
	// }

	var lp int
	for lp = 0; lp < 100; lp++ {
		var k uint32 = (uint32)(rand.Intn(30) % 30)

		s.Get_logger().Debug("update or insert key: " + tools.Uint32tostring(k))
		var valueint *[]byte = newByteableInt(lp)
		if s.Write(k, valueint) != nil {
			break
		}
		// tp.PrintNode(s, s.Get_root_node())
		s.Print(this.log)
	}

	for lp = 0; lp < 100; lp++ {
		var k uint32 = uint32(rand.Intn(30) % 30)
		var data *[]byte
		var ret tools.Ret
		ret, data = s.Read(k)
		if ret != nil {
			break
		}
		this.log.Debug("writing zeroes over existing key block: " + tools.Uint32tostring(k))
		if len(*data) == 0 {
			continue
		}
		// write a bunch of zeroes over it
		var data0 []byte = padto4k(binstringstart(0))

		if ret := s.Write(k, &data0); ret != nil {
			return ret
		}
		s.Diag_dump_block(k)

		s.Print(this.log)
	}

	/* now do a bunch of random inserts and deletes */

	for lp = 0; lp < 100; lp++ {
		for rp := 0; rp < 40; rp++ {
			var k uint32 = uint32(rand.Intn(30) % 30)

			this.log.Debug("write to block key: " + tools.Uint32tostring(k))
			var datak []byte = padto4k(binstringstart(int(k)))

			var ret = s.Write(k, &datak)
			if ret != nil {
				break
			}

			s.Print(this.log)
		}
		// // now delete until there are only 5 left
		// for s.Get_free_position() > 5 {
		// 	var k int = (rand.Intn(30) % 30)
		// 	var key string = tools.Inttostring(0) + tools.Inttostring(k)
		// 	key = key[:len(key)-2]
		// 	var keystr string = newByteableString(key)

		// 	var ret, foundresp, _ = s.Fetch(keystr)
		// 	if ret != nil {
		// 		break
		// 	}
		// 	var b bool = foundresp
		// 	if b == false {
		// 		continue
		// 	}
		// 	this.log.Debug("deleting existing key: " + key)

		// 	if s.Delete(keystr, true) != nil {
		// 		break
		// 	}
		// treeprinter_iii.printNode(s, s.get_root_node());
		// s.Print(this.log)
	}
	return nil
}
