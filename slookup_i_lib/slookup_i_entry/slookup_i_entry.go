// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

// Package slookup_i_lib has a comment
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
	/* to be clear, this array is always maxed out in allocation. what determines how full it is, is where the zeroes
	are. since we're going to be writing this to an entry, we need all array elements allocated and not junk, so
	this array will have trailing zeros to denote a not-full list, and if all entries are non-zero then all are full.
	but something like 24-67-0-23-14 is illegal. once you hit a zero, that's the end of the list. */

	/* when this is serialized, we store its length so each node knows exactly how much data it actually stored in its data block,
	as in, all data_blocks are full except the last one and the length of that one can be determined by length mod data_block_size.
	It is therefore possible to have conflicting information, like an array of length 5 with only one non zero value in it, but
	a value length of more than 2 blocks worth. We will be careful. I mean you can also have a block list in stree which is
	spotted with zeroes, which is also illegal, so we just take care not to do that. */
	/* basically, we don't specifically need a field in this struct to hold the length of the data in this entry,
	we can use the len(value) for that, but we will have to serialize that number to disk and back. */

	value *[]byte /* the actual value data. is not stored in the lookup entry (seralized/deserialize),
	but we keep it here so we have it all in one place. */

	/* This is only kept around for ease of validation, this value is not serialized to disk.
	 * serializing a node means getting the actual size of the block_group_list array and writing that so we can
	 * deserialize correctly. which means this value gets set once at creation and deserializing
	 * does not overwrite it so it better be correct. it is block_size * block_group_count, the amount of data one
	 block_num refers to in bytes, which is different than the max_value_length in slookup_i */
	max_entry_value_length uint32

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

	/* can't be nil, this is the array of size block_group_count that holds the position of the
	lookup table entry that refers to this data_block. */
	data_block_reverse_lookup_list *[]uint32

	/* this is the position in the lookup table that this entry instance refers to. it is not stored on disk,
	and is really only used for logging, but basically we have no idea who we are, except for this. */
	entry_pos uint32
}

func New_slookup_entry(l *tools.Nixomosetools_logger, Entry_pos uint32, Max_entry_value_length uint32,
	Block_group_count uint32) *Slookup_i_entry {

	var n Slookup_i_entry
	n.log = l
	n.value = nil
	// max value length is the block_size * block_group_count
	n.max_entry_value_length = Max_entry_value_length
	// if the array is null then you are an offspring node, otherwise you are a mother node.
	/* we don't serialize this or store it, but we need it to know how big to make the offspring array
	 * when we deserialize a node from disk, the array size has to be the max allowable for this block size.
	 * This is the offspring array size, which does not include the space we can also use in the mother node. */
	// this really can't be zero.
	n.block_group_count = Block_group_count
	var v []uint32 = make([]uint32, Block_group_count) // array is fully allocated but set all to zero
	n.block_group_list = &v
	var r []uint32 = make([]uint32, Block_group_count) // array is fully allocated but set all to zero
	n.data_block_reverse_lookup_list = &r

	// not deserialized from disk, just stored in the struct so an entry can identify itself.
	n.entry_pos = Entry_pos
	return &n
}

func (this *Slookup_i_entry) Dump(with_value bool) string {
	var str = "entry pos: " + tools.Uint32tostring(this.entry_pos) + ", block_group_count: " +
		tools.Uint32tostring(this.block_group_count) +
		", block_group_list: ["
	for pos, n := range *this.block_group_list {
		if pos > 0 {
			str += " "
		}
		str += tools.Uint32tostring(n)
	}
	str += "], max_value_length: " + tools.Uint32tostring(this.max_entry_value_length) +
		", data_block_lookup_list: ["

	for pos, n := range *this.data_block_reverse_lookup_list {
		if pos > 0 {
			str += " "
		}
		str += tools.Uint32tostring(n)
	}
	str += "],  value length: " + tools.Uint32tostring(this.Get_value_length())
	if with_value {
		str += tools.Dump(*this.Get_value())
	}
	return str
}

func (this *Slookup_i_entry) Get_value() *[]byte {
	return this.value
}

func (this *Slookup_i_entry) Init() {
}

func (this *Slookup_i_entry) Set_value(new_value *[]byte) tools.Ret {
	// make sure the value we're setting doesn't exceed the limits we said we can store

	if new_value != nil {
		if uint32(len(*new_value)) > this.max_entry_value_length {
			return tools.Error(this.log, "trying to set value length of ", len(*new_value), " but only have space for  ",
				this.max_entry_value_length)
		}
	}
	this.value = new_value // should we make copy? who owns original memory
	return nil
}

func (this *Slookup_i_entry) Get_block_group_list() *[]uint32 {
	/* give access to the raw block group list array so it can be used for read_list and write_list.
	the alternative is to copy all the values out one by one which is great for pretty interfaces but
	dumb for the purposes of not wasting time for no reason except pretty interfaces */
	// I guess making this const would be a nice compromise
	return this.block_group_list
}

func (this *Slookup_i_entry) Get_block_group_lengthxxxz() uint32 {
	/* return the number of non-zero elements in the block_group_list array. the array is always allocated to its max
	size, but how many elements are used is defined by where the first zero is. */
	var retpos uint32 = 0
	var val uint32 = 0
	// xxxz obviously caching this would be a better idea
	for _, val = range *this.block_group_list {
		if val == 0 {
			break
		}
		retpos++
	}
	return retpos
}

func (this *Slookup_i_entry) Get_value_length() uint32 {
	// return the length of the data in this block_group set
	return uint32(len(*this.value))
}

func (this *Slookup_i_entry) Get_max_value_length() uint32 {
	// return the max length of data
	return this.max_entry_value_length
}

func (this *Slookup_i_entry) Get_entry_pos() uint32 {
	// return the cached entry pos value that this entry represents
	return this.entry_pos
}

/* get and set an item in the block_group array for this lookup entry */

func (this *Slookup_i_entry) Get_block_group_pos(block_group_pos uint32) (tools.Ret, uint32) {
	if block_group_pos > this.block_group_count {
		return tools.Error(this.log, "trying to get block_group_pos ", block_group_pos,
			" which is greater than the block_group count ", this.block_group_count), 0
	}

	/* this is a data-caused error, nobody should be asking for a position in the block_group_list
	for an entry that is outside of the range of data stored in the entry. */
	if block_group_pos > this.Get_block_group_lengthxxxz() {
		return tools.Error(this.log, "trying to get block_group pos ", block_group_pos,
			" which is greater than the block_group list array length ", this.Get_block_group_lengthxxxz()), 0
	}
	return nil, (*this.block_group_list)[block_group_pos]
}

func (this *Slookup_i_entry) Set_block_group_pos(block_group_pos uint32, data_block uint32) tools.Ret {
	/* given an index into the block_group array, set the array position's value after validating */
	if block_group_pos >= this.block_group_count {
		return tools.Error(this.log, "trying to set block_group  pos ", block_group_pos,
			" which is greater than the number of blocks in the block_group ", this.block_group_count)
	}
	/* now this is okay, we should be able to store larger-than-currentl-exists data in the
	block_group_list, the caller must be aware of what's going on and write zeros to the end if they're
	shrinking the number of blocks in the entry */
	// if block_group_pos > uint32(len(*this.block_group_list)) {
	// 	return tools.Error(this.log, "trying to set block_group pos ", block_group_pos,
	// 		" which is greater than the block_group list array length ", len(*this.block_group_list))
	// }
	(*this.block_group_list)[block_group_pos] = data_block
	return nil
}

func (this *Slookup_i_entry) Set_reverse_lookup_pos(reverse_lookup_pos uint32, entry_block_num uint32) tools.Ret {
	/* same as above except instead of setting a block_group_pos, we're setting a reverse_lookup_pos */
	if reverse_lookup_pos > this.block_group_count {
		return tools.Error(this.log, "trying to set reverse_lookup_pos ", reverse_lookup_pos,
			" which is greater than the block_group_count ", this.block_group_count)
	}
	// the reverse lookup arrays are always maxed out to block_group_count length so we don't have to check it. */
	(*this.data_block_reverse_lookup_list)[reverse_lookup_pos] = entry_block_num
	return nil
}

func (this *Slookup_i_entry) Get_reverse_lookup_pos(reverse_lookup_pos uint32) (tools.Ret, uint32) {
	/* same as above except instead of setting a reverse_lookup_pos, we're returning the value */
	if reverse_lookup_pos >= this.block_group_count {
		return tools.Error(this.log, "trying to set reverse_lookup_pos ", reverse_lookup_pos,
			" which is greater than the block_group_count ", this.block_group_count), 0
	}
	// the reverse lookup arrays are always maxed out to block_group_count length so we don't have to check it. */
	return nil, (*this.data_block_reverse_lookup_list)[reverse_lookup_pos]
}

func (this *Slookup_i_entry) Serialized_size() uint32 {
	/* return the size of this node in bytes when it is serialized in the serialize function below
	* ie, add up all the sizes of the fields we're going to serialize.
	stree stores the key and value in the node, slookup does not, so the lookup table entry will always be
	the same size, the block_group list array is always the same size, and is relatively small. */

	var retval uint32 = 4 // value length (so we can restore exactly what was passed to us even if it is less than the block size

	// space needed for block_group array. which is a constant, given the block_group_count.
	retval += uint32(4 * this.block_group_count)
	// space needed for revese lookup array, data_block_lookup_list
	retval += uint32(4 * this.block_group_count)
	return retval
}

func (this *Slookup_i_entry) Serialize() (tools.Ret, *[]byte) {
	/* serialize this node into a byte array, which defines the length of a horribly misaligned lookup table entry */
	/* actually since everything is uint32s now, it's possible to make a block_group_count such that
	a pile of entries will fit neatly in a block */
	var bval *[]byte = this.value
	var ssize uint32 = this.Serialized_size() // see below, we specifically store the length stored not the padded out length
	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0, ssize))

	/* as of 11/22/2020 we're going to try making the value only store the actual size of the value
	* data so all value lengths will be the amount stored, the idea being
	* a) we can reproduce the size the original calling writer sent to us,
	* b) this only applies to the last filled in the block group, all others should be full.
	but the length itself is the full block_group data length, not just the length of the data
	in the last data_block. */
	binary.Write(bb, binary.BigEndian, uint32(len(*bval))) // if you just pass len(bval) it doesn't write anything

	// offspring can never be null, we always allocate the entire block_group_list array

	for rp := 0; rp < int(this.block_group_count); rp++ {
		binary.Write(bb, binary.BigEndian, uint32((*this.block_group_list)[rp]))
	}

	// and then the reverse lookup array
	for rp := 0; rp < int(this.block_group_count); rp++ {
		binary.Write(bb, binary.BigEndian, uint32((*this.data_block_reverse_lookup_list)[rp]))
	}
	if uint32(len(*bval)) > this.max_entry_value_length {
		return tools.Error(this.log, "value length is bigger than max value length: ", this.max_entry_value_length,
			" it is ", len(*bval)), nil
	}

	/* unlike stree, we don't write the actual value here, somebody else is going to have to deal with writing out
	the data to the data area block */

	var bret []byte = bb.Bytes()

	return nil, &bret
}

func (this *Slookup_i_entry) Deserialize(log *tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	/* deserialize incoming data into this entry.
	interesting to note that an unused entry (one that has previously not ever been written to) will
	be all zeroes, we should make sure to deserialize correctly and make the correct zero length value.
	it is for this reason that when initializing an slookup_i data store you must suffer the pain of zeroing
	out the entire lookup table. if there is existing junk in the lookup table storage space, this deserialize
	will make a mess of things. */

	var bpos int = 0
	var bp []byte = *bs

	var data_value_length uint32 = binary.BigEndian.Uint32(bp[bpos:])
	bpos += 4

	var v []uint32 = make([]uint32, this.block_group_count) // array is fully allocated but set all to zero
	this.block_group_list = &v
	for rp := 0; uint32(rp) < this.block_group_count; rp++ { // optimization: just mass copy this and endian flip later if need be
		(*this.block_group_list)[rp] = binary.BigEndian.Uint32(bp[bpos:])
		bpos += 4
	}

	var r []uint32 = make([]uint32, this.block_group_count) // array is fully allocated but set all to zero
	this.data_block_reverse_lookup_list = &r
	for rp := 0; uint32(rp) < this.block_group_count; rp++ { // optimization: mass copy this.
		(*this.data_block_reverse_lookup_list)[rp] = binary.BigEndian.Uint32(bp[bpos:])
		bpos += 4
	}

	/* we serialized the actual length of the data field (not the data), we allocate the value memory here and
	that's how we know how much data to read later, the size of the array stores the value length. */
	/* upon reflection, this is a bit dumb. we're wasting time and memory on an allocation that we may never
	read the value into, just to have somewhere to store the length of the value we may eventually read.
	we should revisit this someday. we can still store zero length values this way though. */

	var value_storage []byte = make([]byte, int(data_value_length))
	this.value = &value_storage

	return nil
}

// func (this *Slookup_i_entry) Count_offspring() uint32 {

// 	// remove this function
// 	return this.Get_block_group_lengthxxxz()

// 	// var rp int // make this faster xxxz
// 	// // by using the size of the array, not padding out to block_group_count and filling with zeroes, that's dumb. fix that xxxz
// 	// for rp = 0; rp < len(*this.block_group_list); rp++ {
// 	// 	if (*this.block_group_list)[rp] == 0 {
// 	// 		return uint32(rp)
// 	// 	}
// 	// }
// 	// return uint32(len(*this.block_group_list)) // if they're all full, then there are as many as there are array elements
// }

// this was implemented directly in the slookup_i reverse lookup function
// func (this *Slookup_i_entry) Find_data_block_in_data_block_lookup_list(data_block uint32) (tools.Ret, uint32) {
// 	/* there's two steps to doing a reverse lookup:
// 	1) given the data_block position, divide by block_group_count to get
// 	the lookup table entry position, read that lookup table entry, get the data block position mod block_group_count to get
// 	the position in the reverse lookup array, then read that value, that's the lookup entry position that should have our
// 	data block pos.
// 	2) read the lookup table entry for that value, and call this function to get the position of that data block
// 	in the block_group list
// 	If you're doing everything right, it MUST be there, so it's an error if it's not.
// 	all the heavy lifting of #1 will be in slookup_i. we're just the entry class. */

// 	// make this faster too, although this won't actually happen much
// 	for pos, n := range *this.block_group_list {
// 		if n == data_block {
// 			return nil, uint32(pos)
// 		}
// 	}
// 	// we really have to start storing the entry position number in the entry. not on disk, just in the structure.
// 	return tools.Error(this.log, "sanity failure: couldn't find data_block: ", data_block, " in block_group_list, for entry: ",
// 		this.entry_pos), 0
// }
