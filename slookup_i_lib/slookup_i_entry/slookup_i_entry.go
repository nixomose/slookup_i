// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package slookup_i_lib

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/nixomose/nixomosegotools/tools"
)

type Slookup_entry struct {
	log *tools.Nixomosetools_logger
	/* unlike stree, we don't store keys and values, we store block positions and data at that block position
	and the data is exactly block_size length and fits nicely aligned on disk. the lookup table, not so much
	with the nice and aligned, but at least the data is.
	this is the definition of the lookup table entry, it has the information defining a mother or offspring block
	and will be unpleasantly misaligned. */

	data_block_num   uint32 // the block number (offset from zero in the backing store) of where this entry's position data is (mother node only if a parent)
	mother_block_num uint32 // if this block is a mother node, this is zero, if this block is an offspring block it points to this offspring's mother block num

	/* offspring in slookup work basically the same way as they do in stree.
	 * a block num refers to an amount of data block_size.
	 * a lookup table entry therefore has the block number for where this data is and information about how to find it's mother
	 * if it is an offspring block.
	 * if this block is a mother block, then the offspring list points to the blocks making up the offspring for this mother block.
	 * if the mother_block_num is zero (denoting this block is an offspring) and the offspring are all zero that just means
	 * that all the data for this mother block fits in the mother node, and no offspring were allocated. */

	/* offspring is an array of size m_offspring_per_node.
	 * it represents a list of other nodes that store data (in order) in the mother node.
	 * the value in the array will be filled from 0 up to max, and if the value
	 * is zero then that's the end of the list, if the last element is not zero
	 * then it is a maxed out block, that means zero does not denote the end of the list
	 * unless the block being stored doesn't use all the offspring nodes.
	 * This scheme allows us to store a 64k block that compresses down to 4 in one block
	 * instead of 16, so we can save lots of space.
	 * this also gives us a lot of variability in the range of size blocks we can efficiently store.
	 * The first element in the offspring node is NOT the mother node, it is the first offspring node,
	 * so if the array is all zeroes, then only the mother node holds data.
	 *
	 * this also means that value length is only the value length of THIS node,
	 * as a mother or offspring, it only refers to this node, so the mother and
	 * all but the last child will be the block size, and only the last offspring
	 * node will be less than block_size in length. of course if the stored amount
	 * is less than one block size, then yea the mother node will also be short.
	 *
	 * Like the stree, the caller will supply a node block size (the smallest thing you will store)
	 * and the number of offspring you want, and we will tell you the max size block you
	 * can store. So be it. */
	offspring_nodes uint32 // how many elements in the offspring array, always fully allocated to max for block size
	/* you always get one node (the mother) this is how many additional nodes you want in this block */
	offspring *[]uint32 // nil (for offspring nodes) or array of size offspring_nodes

	// when this is serialized, we store it's length so each node knows exactly how much data it actually stored in its data block
	value []byte /* the actual value data is not stored in the lookup entry (seralized/deserialize),
	but we keep it here so we have it all in one place. */

	/* This is only kept around for ease of validation, this value is not serialized to disk.
	 * serializing a node means getting the actual size of the value and writing that so we can
	 * deserialize correctly. which means this value gets set once at creation and deserializing
	 * does not overwrite it so it better be correct. it is block_size, the amount of data one
	 block_num refers to in bytes. */
	max_value_length uint32
}

func New_slookup_entry(l *tools.Nixomosetools_logger, Val []byte, Max_value_length uint32,
	Additional_offspring_nodes uint32) *Slookup_entry {

	var n Slookup_entry
	n.log = l
	n.value = Val
	n.data_block_num = 0
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

func (this *Slookup_entry) Get_value() []byte {
	return this.value
}

func (this *Slookup_entry) Init() {
}

func (this *Slookup_entry) Set_value(new_value []byte) tools.Ret {
	// make sure the value we're setting doesn't exceed the limits we said we can store

	if uint32(len(new_value)) > this.max_value_length {
		return tools.Error(this.log, "trying to set value length of ", len(new_value), " but only have space for  ", this.max_value_length)
	}
	this.value = new_value // should we make copy? who owns original memory
	return nil
}

func (this *Slookup_entry) Get_mother_block_num() uint32 {
	/* unlike stree, there is no parent, there is only the mother block for which this offspring is a part of (if it is
	an offspring node) */
	return this.mother_block_num
}

func (this *Slookup_entry) Get_value_length() uint32 {
	return this.max_value_length
}

func (this *Slookup_entry) Set_mother_block_num(spos uint32) {
	this.mother_block_num = spos
}

func (this *Slookup_entry) Set_offspring_pos(offspring_pos uint32, node_pos uint32) tools.Ret {
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

func (this *Slookup_entry) Is_offspring() bool {
	/* 6/22/2021 the node's offspring variable is non null if we're the mother node because the mother has a list
	 * of offspring, and the offspring do not. However, we missed a case where if the whole stree is not set up
	 * to use offspring, then offspring is null in the mother node as well. */
	if this.offspring_nodes == 0 {
		return false
	}
	if this.offspring == nil {
		return true
	}
	return false
}

func (this *Slookup_entry) Get_offspring_pos(offspring_pos uint32) (tools.Ret, *uint32) {
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

func (this *Slookup_entry) Serialized_size() uint32 {
	/* return the size of this node in bytes when it is serialized in the serialize function below
	* ie, add up all the sizes of the fields we're going to serialize.
	stree stores the key and value in the node, slookup does not, so the lookup table entry will always be
	the same size, (except for offspring array) and is relatively small as there's no data in it at all. */

	var retval uint32 = 4 + // data_block_num
		4 + // mother_block_num
		4 + // value length (so we can restore exactly what was passed to us even if it is less than the block size
		4 // space needed to store the number of items in the offspring array (or 0 or max_int32)

	/* this is interesting, this gives you the serialized size of THIS node, but if you want to know
	how to size the biggest node you're going to ever need (so you know know the byte size of a lookup entry)
	you must call this with a mother node otherwise you'll get the wrong size. */
	// space needed for offspring array, if any, for this node. this is so we can size the allocation to serialize into.
	if this.offspring != nil {
		retval += uint32(4 * len(*this.offspring))
	}
	return retval
}

func (this *Slookup_entry) Serialize() (tools.Ret, *bytes.Buffer) {
	/* serialize this node into a byte array, which defines the length of a horribly misaligned lookup table entry */
	var bval []byte = this.value
	var ssize uint32 = this.Serialized_size() // see below, we specifically store the length stored not the padded out length
	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0, ssize))
	binary.Write(bb, binary.BigEndian, this.data_block_num)
	binary.Write(bb, binary.BigEndian, this.mother_block_num)

	/* as of 11/22/2020 we're going to try making the value only store the actual size of the value
	 * data so all value lengths will be the amount stored, the idea being
	 * a) we can reproduce the size the original calling writer sent to us,
	 * b) this only applies to the last node in the block, all others should be full. */
	binary.Write(bb, binary.BigEndian, uint32(len(bval))) // if you just pass len(bval) it doesn't write anything

	/* we have to distinguish between a zero length array and no offspring array at all (a mother node
	 * versus an offspring node)*/
	if this.offspring != nil {
		binary.Write(bb, binary.BigEndian, uint32(len(*this.offspring)))

		for rp := 0; rp < len(*this.offspring); rp++ {
			binary.Write(bb, binary.BigEndian, uint32((*this.offspring)[rp]))
		}
	} else {
		binary.Write(bb, binary.BigEndian, uint32(math.MaxUint32)) // flag for offspring node, again you must cast or write doesn't do anything.
	}
	if uint32(len(bval)) > this.max_value_length {
		return tools.Error(this.log, "value length is bigger than max value length: ", this.max_value_length, " it is ", len(bval)), nil
	}

	/* unlike stree, we don't write the actual value here, somebody else is going to have to deal with writing out
	the data to the data area block */

	return nil, bb
}

func (this *Slookup_entry) Deserialize(log tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	/* deserialize incoming data into this entry */

	var bpos int = 0
	var bp []byte = *bs

	this.data_block_num = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	this.mother_block_num = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4
	var data_value_length uint32 = binary.BigEndian.Uint32(bp[bpos:])
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
	/* we serialized the actual length of the data field (not the data), we allocate the value memory here and
	that's how we know how much data to read later, the size of the array stores the value length. */

	this.value = bp[bpos : bpos+int(data_value_length)]
	return nil
}

func (this *Slookup_entry) Count_offspring() uint32 {
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
