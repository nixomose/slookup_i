// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package slookup_i_lib

import (
	"bytes"
	"encoding/binary"

	"github.com/nixomose/nixomosegotools/tools"
)

type Slookup_i_entry struct {
	log *tools.Nixomosetools_logger

	/* unlike stree, we don't store keys and values, we store block positions and data at that block position
	and the data is exactly block_size length and fits nicely aligned on disk. the lookup table, not so much
	with the nice and aligned, but at least the data is.
	this is the definition of the lookup table entry, it has the information defining all the data_blocks making up
	the block_group that this lookup table entry refers to.	and it will be unpleasantly misaligned.
	So I realize now the idea of mother and offpsring is not applicable, there are no offspring node lookup table entries.
	there are only lookup table entries with the list of data_blocks that comprise the block_group. this is way way
	simpler than stree.	*/

	// data_block_num   uint32 // the block number (offset from zero in the backing store) of where this entry's position data is (mother node only if a parent)
	// mother_block_num uint32 // if this block is a mother node, this is zero, if this block is an offspring block it points to this offspring's mother block num

	/* offspring in slookup work sorta the same way as they do in stree.
	 * a block num refers to an amount of data block_size * num_offspring.
	 * a lookup table entry therefore has the list of data block numbers for where all the data in the block_group is. */

	/* I think maybe we won't call it offspring anymore, but block_group */
	/* block_group is an array of size m_offspring_per_node.
	 * it represents the list of data_block block_nums where the data for this block_group is stored (in order).
	 * the value in the array will be filled from 0 up to max, and if the value
	 * is zero then that's the end of the list, if the last element is not zero
	 * then it is a maxed out block, that means zero does not denote the end of the list
	 * unless the block being stored doesn't use all the block_group entries.
	 * This scheme allows us to store a 64k block that compresses down to 4k in one block
	 * instead of 16, so we can save lots of space.
	 * this also gives us a lot of variability in the range of size blocks we can efficiently store.
	 * so if the array is all zeroes, we store nothing? hmmm... I guess that happens if we trim everything.
	 * so yes, it is possible to have a lookup table with no block_group entries in it.
	 *
	 * this also means that value length is value length of the whole block_group so we know how much data is in the last
	 * possibly unfilled allocated block_group block.
	 * only the last block_group entry will be less than block_size in length. it is possible for the block_group length to be zero.
	 */
	block_group_count uint32    // how many elements in the block_group array, always fully allocated to max for block_group size
	block_group_list  *[]uint32 // can't be nil, this is the array of size block_group_count

	/* when this is serialized, we store its length so each node knows exactly how much data it actually stored in its data block,
	as in, all data_blocks are full except the last one and the length of that one can be determined by length mod data_block_size.
	It is therefore possible to have conflicting information, like an array of length 5 with only one non zero value in it, but
	a value length of more than 2 blocks worth. We will be careful. I mean you can also have a block list in stree which is
	spotted with zeroes, which is also illegal, so we just take care not to do that. */

	value []byte /* the actual value data. is not stored in the lookup entry (seralized/deserialize),
	but we keep it here so we have it all in one place. */

	/* This is only kept around for ease of validation, this value is not serialized to disk.
	 * serializing a node means getting the actual size of the block_group_list array and writing that so we can
	 * deserialize correctly. which means this value gets set once at creation and deserializing
	 * does not overwrite it so it better be correct. it is block_size * block_group_count, the amount of data one
	 block_num refers to in bytes. */
	max_value_length uint32

	/* so now we're left with the problem of when we need to move a data_block we have to find the lookup table
	entry that points to it. took me a while to figure out but what we can do is have another lookup table that instead
	of mapping lookup table entries to datablocks, maps data blocks by position to lookup entries. Now... where to put this
	new list. We can put it after the lookup table, because then we can't resize the lookup table.
	But we CAN put it IN the lookup table. So that's here.
	The first problem I ran into was that there are going to be more data blocks than there are lookup entries.
	In fact there are going to be exactly block_group_count-1 times more data blocks than lookup entries.
	so instead of having a value that maps a data block position to a lookup table position, we have an array
	and we spread all the data_block positions over all the lookup table entries, because there AREN'T going
	to be more data blocks than lookup entries * block_group_count. So when we get the right lookup entry with the
	reverse lookup position in it, we mod by the array size to get the position, we pluck the value out of that
	array position and that is the lookup entry position to find that block in. we load THAT entry and scan the
	block_group array for the block we're reverse looking up.

	So lets say we have a block_group_count of 5 and 10 lookup entries, meaning there are 50 data blocks total.
	entry #0 will have the data_block position lookup for data blocks 0-5, entry #1 will have 5-10, and so on.
	so to do a reverse lookup, you take the data block position, divide by 5, to get the lookup table entry position
	then mod 5 to get the position in the data_block_lookup array. the value in that position will be the position
	of the lookup entry that has that data_block listed in it. So we read that lookup entry in and scan the
	block_group array for the data_block position we're looking for, and it has to be there. */
	data_block_lookup_list *[]uint32 // can't be nil, this is the array of size block_group_count that holds the position of the
	//	lookup table entry that refers to this data_block

}

func New_slookup_entry(l *tools.Nixomosetools_logger, Max_value_length uint32, Block_group_count uint32) *Slookup_i_entry {

	var n Slookup_i_entry
	n.log = l
	n.value = nil
	n.max_value_length = Max_value_length
	// if the array is null then you are an offspring node, otherwise you are a mother node.
	/* we don't serialize this or store it, but we need it to know how big to make the offspring array
	 * when we deserialize a node from disk, the array size has to be the max allowable for this block size.
	 * This is the offspring array size, which does not include the space we can also use in the mother node. */
	// this really can't be zero.
	n.block_group_count = Block_group_count
	var v []uint32 = make([]uint32, Block_group_count) // array is fully allocated but set all to zero
	n.block_group_list = &v
	return &n
}

func (this *Slookup_i_entry) Get_value() []byte {
	return this.value
}

func (this *Slookup_i_entry) Init() {
}

func (this *Slookup_i_entry) Set_value(new_value []byte) tools.Ret {
	// make sure the value we're setting doesn't exceed the limits we said we can store

	if uint32(len(new_value)) > this.max_value_length {
		return tools.Error(this.log, "trying to set value length of ", len(new_value), " but only have space for  ", this.max_value_length)
	}
	this.value = new_value // should we make copy? who owns original memory
	return nil
}

func (this *Slookup_i_entry) Get_data_block_pos(block_group_pos uint32) uint32 {
	/* return the block num this entry refers to where it's data is. */
	return (*this.block_group_list)[block_group_pos]
}

func (this *Slookup_i_entry) Get_value_length() uint32 {
	return this.max_value_length
}

func (this *Slookup_i_entry) Set_block_group_pos(block_group_pos uint32, data_block uint32) tools.Ret {
	if block_group_pos > this.block_group_count {
		return tools.Error(this.log, "trying to set block_group  pos ", block_group_pos,
			" which is greater than the number of blocks in the block_group ", this.block_group_count)
	}
	if block_group_pos > uint32(len(*this.block_group_list)) {
		return tools.Error(this.log, "trying to set block_group pos ", block_group_pos,
			" which is greater than the block_group list array length ", len(*this.block_group_list))
	}
	(*this.block_group_list)[block_group_pos] = data_block
	return nil
}

func (this *Slookup_i_entry) Get_block_group_pos(block_group_pos uint32) (tools.Ret, *uint32) {
	if block_group_pos > this.block_group_count {
		return tools.Error(this.log, "trying to get block_group_pos ", block_group_pos,
			" which is greater than the block_group count ", this.block_group_count), nil
	}
	if block_group_pos > uint32(len(*this.block_group_list)) {
		return tools.Error(this.log, "trying to get block_group pos ", block_group_pos,
			" which is greater than the block_group list array length ", len(*this.block_group_list)), nil
	}
	return nil, &(*this.block_group_list)[block_group_pos]
}

func (this *Slookup_i_entry) Serialized_size() uint32 {
	/* return the size of this node in bytes when it is serialized in the serialize function below
	* ie, add up all the sizes of the fields we're going to serialize.
	stree stores the key and value in the node, slookup does not, so the lookup table entry will always be
	the same size, the block_group list array is always the same size, and is relatively small. */

	var retval uint32 = 4 // value length (so we can restore exactly what was passed to us even if it is less than the block size

	// space needed for block_group array. which is a constant, given the block_group_count.
	retval += uint32(4 * this.block_group_count)
	return retval
}

func (this *Slookup_i_entry) Serialize() (tools.Ret, *bytes.Buffer) {
	/* serialize this node into a byte array, which defines the length of a horribly misaligned lookup table entry */
	/* actually since everything is uint32s now, it's possible to make a block_group_count such that
	a pile of entries will fit neatly in a block */
	var bval []byte = this.value
	var ssize uint32 = this.Serialized_size() // see below, we specifically store the length stored not the padded out length
	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0, ssize))

	/* as of 11/22/2020 we're going to try making the value only store the actual size of the value
	* data so all value lengths will be the amount stored, the idea being
	* a) we can reproduce the size the original calling writer sent to us,
	* b) this only applies to the last filled in the block group, all others should be full.
	but the length itself is the full block_group data length, not just the length of the data
	in the last data_block. */
	binary.Write(bb, binary.BigEndian, uint32(len(bval))) // if you just pass len(bval) it doesn't write anything

	// offspring can never be null, we always allocate the entire block_group_list array

	for rp := 0; rp < int(this.block_group_count); rp++ {
		binary.Write(bb, binary.BigEndian, uint32((*this.block_group_list)[rp]))
	}
	if uint32(len(bval)) > this.max_value_length {
		return tools.Error(this.log, "value length is bigger than max value length: ", this.max_value_length, " it is ", len(bval)), nil
	}

	/* unlike stree, we don't write the actual value here, somebody else is going to have to deal with writing out
	the data to the data area block */

	return nil, bb
}

func (this *Slookup_i_entry) Deserialize(log *tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	/* deserialize incoming data into this entry */

	var bpos int = 0
	var bp []byte = *bs

	var data_value_length uint32 = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4

	var v []uint32 = make([]uint32, this.block_group_count) // array is fully allocated but set all to zero
	this.block_group_list = &v
	for rp := 0; uint32(rp) < this.block_group_count; rp++ {
		(*this.block_group_list)[rp] = binary.BigEndian.Uint32(bp[bpos:])
		bpos += 4
	}
	/* we serialized the actual length of the data field (not the data), we allocate the value memory here and
	that's how we know how much data to read later, the size of the array stores the value length. */

	this.value = make([]byte, int(data_value_length))
	return nil
}

func (this *Slookup_i_entry) Count_offspring() uint32 {

	var rp int // make this faster xxxz
	for rp = 0; rp < len(*this.block_group_list); rp++ {
		if (*this.block_group_list)[rp] == 0 {
			return uint32(rp)
		}
	}
	return uint32(len(*this.block_group_list)) // if they're all full, then there are as many as there are array elements
}
