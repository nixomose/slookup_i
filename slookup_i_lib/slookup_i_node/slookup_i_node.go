// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package slookup_i_lib

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/nixomose/nixomosegotools/tools"
)

type Stree_node struct {
	log         *tools.Nixomosetools_logger
	parent      uint32 // position in storage array of paren:t, 0 = root
	left_child  uint32 // position in storage array of the left child node
	right_child uint32 // position in storage array of the right child node
	key         string // keytype

	/* This requires a lot of explanation.
	 * offspring is an array of size m_offspring_per_node defined in stree.
	 * it represents a list of other nodes that store data (in order)
	 * the value in the array will be filled from 0 up to max, and if the value
	 * is zero then that's the end of the list, if the last element is not zero
	 * then it is a maxed out block, that means zero does not denote the end of the list
	 * unless the block being stored doesn't use all the offspring nodes.
	 * This scheme allows us to store a 64k block that compresses down to 4 in one block
	 * instead of 16, so we can save lots of space.
	 * this also gives us a lot of variability in the range of size blocks we can efficiently store.
	 * When we get to stree_v it will get even more flexible.
	 * But for now, since the goal is to be able to store 4k efficiently, that means that the
	 * block size will be 4k, so one 4k block fits in 4k+metadata and the max size 'block' you can store
	 * will be 4k * the number of offspring nodes you set, 16 is probably a pretty good number,
	 * making the max blocksize 64k.
	 * The first element in the offspring node is NOT the mother node, it is the first offspring node,
	 * so if the array is all zeroes, then only the mother node holds data.
	 *
	 * this also means that value length is only the value length of THIS node,
	 * as a mother or offspring, it only refers to this node, so the mother and
	 * all but the last child will be the block size, and only the last offspring
	 * node will be less than block_size in length. of course if the stored amount
	 * is less than one block size, then yea the mother node will also be short.
	 *
	 * So the big question is how do we define the block size. Is it the size of data you
	 * can fit in one node, or the max size of data you can store given the defined
	 * size of the offspring array? Which one do you specify, the max size and the number
	 * of pieces you want? or the smallest size and the multiple and we calculate the max.
	 * Both have one thing that works and one that makes no sense.
	 * I need to store 4k, so I say okay, the 4k is the node size and I want 15 of them
	 * but then I have to ask the stree to find out what the actual max block size is.
	 * And that's kinda weird.
	 * Or I can give it the max block size, and number of divisions, and I just have to make sure
	 * it magically comes out to the node size being 4k, and then I can never change it to make it
	 * bigger. Actually I can't change it anyway, because that would change the on disk layout
	 * so there's no offspring array size changing anyway.
	 * I think I'm leaning towards the 4k and the offspring array size, it makes it easier to
	 * work everything else out rather than dividing and possibly screwing it up.
	 * So caller will supply a node block size (the smallest thing you will store)
	 * and the number of offspring you want, and we will tell you the max size block you
	 * can store. So be it. */
	offspring_nodes uint32 // how many elements in the offspring array, always fully allocated to max for block size
	/* you always get one node (the mother) this is how many additional nodes you want in this block */
	offspring *[]uint32 // nil or array of size offspring_nodes

	// value goes at the end so when serialized to disk we can read the header without having to read the value
	value []byte // valuetype

	/* These are only kept around for ease of validation, these values are not serialized to disk.
	 * serializing a node means getting the actual size of the value and writing that so we can
	 * deserialize correctly. which means these values get set once at creation and deserializing
	 * does not overwrite them so they better be correct. */
	max_key_length   uint32
	max_value_length uint32
}

// // verify that Stree_node implements Stree_node_interface
// var _ Stree_node_interface = &Stree_node{}
// var _ Stree_node_interface = (*Stree_node)(nil)

func New_Stree_node(l *tools.Nixomosetools_logger, Key string, Val []byte, Max_key_length uint32,
	Max_value_length uint32, Additional_offspring_nodes uint32) *Stree_node {

	var n Stree_node
	n.log = l
	n.key = Key
	n.value = Val
	n.parent = 0
	n.left_child = 0
	n.right_child = 0
	n.max_key_length = Max_key_length
	n.max_value_length = Max_value_length
	// if the array is null then you are an offspring node, otherwise you are a mother node.
	/* we don't serialize this or store it, but we need it to know how big to make the offspring array
	 * when we deserialize a node from disk, the array size has to be the max allowable for this block size.
	 * This is the offspring array size, which does not include the space we can also use in the mother node. */
	n.offspring_nodes = Additional_offspring_nodes
	if Additional_offspring_nodes > 0 {
		var v []uint32 = make([]uint32, Additional_offspring_nodes) // array is fully allocated but set all to zero
		n.offspring = &v
	} else {
		n.offspring = nil
	}
	return &n
}

func (this *Stree_node) Get_key() string {
	return this.key
}

func (this *Stree_node) Get_value() []byte {
	return this.value
}

func (this *Stree_node) Set_key(new_key string) {
	this.key = new_key
}

func (this *Stree_node) Init() {
}

func (this *Stree_node) Set_value(new_value []byte) tools.Ret {
	// make sure the value we're setting doesn't exceed the limits we said we can store

	if uint32(len(new_value)) > this.max_value_length {
		return tools.Error(this.log, "trying to set value length of ", len(new_value), " but only have space for  ", this.max_value_length)
	}
	this.value = new_value // should we make copy? who owns original memory
	return nil
}

// func (n *Stree_node) toString() string {
// 	ret := "" + n.Get_key()
// 	return ret
// }

// func (n *Stree_node) compareTo(a Stree_node) int {
// 	/* go's compareTo, is kinda dumb. */
// 	if n.Get_key() == a.Get_key() {
// 		return 0
// 	}
// 	if n.Get_key() < a.Get_key() {
// 		return -1
// 	}
// 	return 1
// }

func (this *Stree_node) Get_parent() uint32 {
	return this.parent
}

func (this *Stree_node) Get_left_child() uint32 {
	return this.left_child
}

func (this *Stree_node) Get_right_child() uint32 {
	return this.right_child
}

func (this *Stree_node) Get_key_length() uint32 {
	return this.max_key_length
}
func (this *Stree_node) Get_value_length() uint32 {
	return this.max_value_length
}

// do we need this?
//    public int[] get_offspring()
//      {
//        return offspring;
//      }

func (this *Stree_node) Set_parent(spos uint32) {
	this.parent = spos
}

func (this *Stree_node) Set_left_child(spos uint32) {
	this.left_child = spos
}

func (this *Stree_node) Set_right_child(spos uint32) {
	this.right_child = spos
}

// I doubt we'll ever need this, we'll only modify the existing array, which is always full size.
//    public void set_offspring(int o[])
//      {
//        offspring = o;
//      }

func (this *Stree_node) Set_offspring_pos(offspring_pos uint32, node_pos uint32) tools.Ret {
	if offspring_pos > this.offspring_nodes {
		return tools.Error(this.log, "trying to set offspring pos ", offspring_pos,
			" which is greater than the number of offpsring nodes ", this.offspring_nodes)
	}
	if offspring_pos > uint32(len(*this.offspring)) {
		return tools.Error(this.log, "trying to set offspring pos ", offspring_pos, " which is greater than the offpsring array length ", len(*this.offspring))
	}
	(*this.offspring)[offspring_pos] = node_pos
	return nil
}

func (this *Stree_node) Is_offspring() bool {
	/* 6/22/2021 the node's offspring variable is non null of we're the mother node because the mother has a list
	 * of offspring, and the offspring do not. However, we missed a case where if the whole stree is not set up
	 * to use offspring, then offspring is null in the mother node as well. But the node has no concept of
	 * the state of the stree it is part of, so all callers of this function have to check for themselves.
	 * okay, that's not true, apparently we store the number of offspring nodes in the stree. how handy. */
	if this.offspring_nodes == 0 {
		return false
	}
	if this.offspring == nil {
		return true
	}
	return false
}

func (this *Stree_node) Get_offspring_pos(offspring_pos uint32) (tools.Ret, *uint32) {
	if offspring_pos > this.offspring_nodes {
		return tools.Error(this.log, "trying to get offspring pos ", offspring_pos,
			" which is greater than the number of offpsring nodes ", this.offspring_nodes), nil
	}
	if offspring_pos > uint32(len(*this.offspring)) {
		return tools.Error(this.log, "trying to get offspring pos ", offspring_pos,
			" which is greater than the offpsring array length ", len(*this.offspring)), nil
	}
	return nil, &(*this.offspring)[offspring_pos]
}

func (this *Stree_node) Serialized_size_without_value(key_length uint32, value_length uint32) uint32 {
	var serialized_size uint32 = this.Serialized_size(key_length, value_length) - value_length
	// we are still serializing and returning the length of the value as part of the header
	return serialized_size
}

func (this *Stree_node) Serialized_size(key_length uint32, value_length uint32) uint32 {
	/* return the size of this node in bytes when it is serialized in the serialize function below
	 * ie, add up all the sizes of the fields we're going to serialize. this includes the variable size of the
	 * key and value. which is why they are passed in. */
	var retval uint32 = 4 + // parent
		4 + // left child
		4 + // right child
		4 + // key length (we need to serialize this too)
		4 + // value length (so we can restore exactly what was passed to us even if it is less than the block size
		key_length + //  space needed for key
		value_length + // space needed for value
		4 // space needed to store the number of items in the offspring array (or 0 or max_int)

	if this.offspring != nil {
		retval += uint32((4 * len(*this.offspring)))
	}
	// space needed for offspring array, if any
	return retval
}

func (this *Stree_node) Serialize() (tools.Ret, *bytes.Buffer) {
	/* serialize this node into a byte array, of exactly block_size size */
	// byte[] bkey = key.to_bytes();
	// byte[] bval = value.to_bytes();
	var bkey string = this.key
	var bval []byte = this.value
	var ssize uint32 = this.Serialized_size(uint32(len(bkey)), uint32(len(bval))) // see below, we specifically store the length stored not the padded out length
	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0, ssize))
	// var bp []byte = bb.Bytes()
	binary.Write(bb, binary.BigEndian, this.parent)
	binary.Write(bb, binary.BigEndian, this.left_child)
	binary.Write(bb, binary.BigEndian, this.right_child)
	binary.Write(bb, binary.BigEndian, (uint32(len(bkey)))) // if you just pass len(bkey) it doesn't write anything

	/* as of 11/22/2020 we're going to try making the value only store the actual size of the value
	 * data so all value lengths will be the amount stored, the idea being
	 * a) we can reproduce the size the original calling writer sent to us,
	 * b) this only applies to the last node in the block, all others should be full.
	 * c) since string is used for key and value this holds for keys too which means
	 *    searching on keys might work differently as they're not padded out for you. */
	binary.Write(bb, binary.BigEndian, uint32(len(bval)))
	/* we have to distinguish between a zero length array and no offspring array at all (a mother node
	 * versus an offspring node */
	if this.offspring != nil {
		binary.Write(bb, binary.BigEndian, uint32(len(*this.offspring)))

		for rp := 0; rp < len(*this.offspring); rp++ {
			binary.Write(bb, binary.BigEndian, uint32((*this.offspring)[rp]))
		}
	} else {
		binary.Write(bb, binary.BigEndian, uint32(math.MaxUint32)) // flag for offspring node, again you must cast or write doesn't do anything.
	}
	if uint32(len(bkey)) > this.max_key_length {
		return tools.Error(this.log, "key length is bigger than max key length: ", this.max_key_length, " it is ", len(bkey)), nil
	}
	if uint32(len(bval)) > this.max_value_length {
		return tools.Error(this.log, "value length is bigger than max value length: ", this.max_value_length, " it is ", len(bval)), nil
	}
	binary.Write(bb, binary.BigEndian, []byte(bkey))

	// write the value at the end so we don't have to read it to do tree searches
	binary.Write(bb, binary.BigEndian, []byte(bval))
	if uint32(bb.Len()) != ssize {
		return tools.Error(this.log, "serialization failed, preallocated size: ", ssize, " actual serialized buffer: ", (bb.Len())), nil
	}
	return nil, bb
}

func (this *Stree_node) Deserialize_without_value(log tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	return this.deserialize_runner(log, bs, false)
}

func (this *Stree_node) Deserialize(log tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	return this.deserialize_runner(log, bs, true)
}

func (this *Stree_node) deserialize_runner(log tools.Nixomosetools_logger, bs *[]byte, include_value bool) tools.Ret {
	/* deserialize incoming data into this node. */

	var bpos int = 0
	var bp []byte = *bs

	this.parent = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	this.left_child = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	this.right_child = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	var field_key_length uint32 = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	var field_value_length uint32 = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	var offspring_length uint32 = binary.BigEndian.Uint32(bp[bpos:]) // this is how many we serialized, not the max number of offspring we can have.
	bpos += 4

	if offspring_length == math.MaxUint32 { // flag for offspring node.
		this.offspring = nil
	} else {

		var v []uint32 = make([]uint32, this.offspring_nodes) // array is fully allocated but set all to zero
		this.offspring = &v
		for rp := 0; uint32(rp) < offspring_length; rp++ {
			(*this.offspring)[rp] = binary.BigEndian.Uint32(bp[bpos:])
			bpos += 4
		}
	}
	/* we serialized the actual length of these fields so that's all we read in.
	 * this has nothing to do with the max_key/value_length, those are not restored on deserialize they are
	 * set once at construction and never change. */
	var bkey string = string(bp[bpos : bpos+int(field_key_length)])
	bpos += int(field_key_length)

	if include_value {
		var bval []byte = bp[bpos : bpos+int(field_value_length)]
		bpos += int(field_value_length)
		this.value = bval
	} else {
		this.value = nil
	}
	this.key = bkey
	return nil
}

func (this *Stree_node) Count_offspring() uint32 {
	if this.offspring == nil {
		return 0
	}

	var rp int // make this faster xxxz
	for rp = 0; rp < len(*this.offspring); rp++ {
		if (*this.offspring)[rp] == 0 {
			return uint32(rp)
		}
	}
	return uint32(len(*this.offspring)) // if they're all full, then there are as many as there are array elements
}
