# -*- coding: utf-8 -*-
##########################################################################
# Copyright 2018 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

from __future__ import print_function
import argparse

import aerospike
from aerospike import exception as as_exceptions
'''
This provides a rough implementation of an expandable list for Aerospike
It utilizes a metadata record to provide information about associated subrecords.
It should be considered to be roughly accurate, as distruptions and timeouts can
cause stale data and pointers to be referenced.

The rough algorithm to add an item is:

1. Read top level metadata record to find a subrecord into which an item should be stored. If this fails
   Create the top level record.
2. Try to store the items in the appropriate subrecord.
3. If step 2 fails because the subrecord is too big, create a new subrecord with the given items,
   and increment the metadata record's reference count.

Caveats:

    1. Due to records potentially being stored on different nodes, it is possible that the
    metadata record exists, and is reachable, but certain subrecords are not reachable.
    2. In case of disruptions it is possible to have a subrecord created and not have a reference
    to it stored in the top level metadata record.
    3. No ordering can be assumed, as various delays between additions of items may cause them to be
        stored out of orer.
    4. Fetching all subrecords, will only fetch subrecords known about at the time of the request. Subrecords created after
        the metadata record has been read will not be returned.
    5. This design does not facilitate a way to remove items from the conceptual list.
    6. This design does not allow for writing a single record larger than the write-block-size.

The top level record is roughly
{
    'subrecord_count': #
}

Where the # is greater than or less than or equal to the number of subrecords

each subrecord is roughly:
{
    'items': [item10, item2, item5, item0, item100]
}
'''


class ASMetadataRecordTooLarge(Exception):
    pass


class ClientSideBigList(object):
    '''
    Abstraction around an unbounded size list for Aerospike. Relies on a top level record
    containing metadata about subrecords. When a subrecord fills up, a new subrecord is created
    and the metadata is updated.
    '''

    def __init__(self, client, base_key, ns='test', setname='demo',
                 subrecourd_count_name='sr_count', subrecord_list_bin='items'):
        '''
        Args:
            client (Aerospike.client): a connected client to be used to talk with the database
            base_key (string): The base key around which all record keys will be constructed
                if base_key is 'person1', the top record will have the key (ns, set, 'person1')
                and subrecords will be of the form (ns, set, 'person1-1'), (ns, set, 'person1-2')...
            ns (string): The namespace into which the records will be stored.
            setname (string): The set into which the records will be stored.
            subrecord_count_name (string): The name of the bin in the metadata record which will store the count
                of subrecords.
            subrecord_list_bin (string): The name of the list bin in each of the subrecords.
        '''
        self.ns = ns
        self.set = setname
        self.client = client
        self.base_key = base_key
        self.metadata_key = (self.ns, self.set, self.base_key)

        # This is the name of the bin in the top record which contains a set of subrecords
        # There are at least this many subrecords. Unless the value of this is 1, in which case there may be no
        # subrecords.
        self.subrecourd_count_name = subrecourd_count_name

        # This is the name of the bin containing items in each of the subrecords
        self.subrecord_list_bin = subrecord_list_bin

    def get_metadata_record(self):
        '''
        Fetches the top level record containing metadata about this list.
        Returns None if the record does not exist

        Returns:
            Tuple: The record tuple: (key, meta, bins) for the metadata record if found
            None: if the record does not exist.
        Raises:
            RecordNotFound: If the metadata record for this list does not yet exist.
            AerospikeError: If the operation fails
        '''
        return self.client.get(self.metadata_key)

    def add_item(self, item):
        '''
        Add a given item to this conceptual group of lists. If a top level
        record has not yet been created, this operation will create it.

        Args:
            item: The item to be stored into the list.
        Raises:
            AerospikeError:
                If communication with the cluster fails.
        '''

        try:
            _, meta, bins = self.get_metadata_record()
            generation = meta['gen']
            subrecord_count = bins[self.subrecourd_count_name]
            subrecord_count = max(1, subrecord_count)  # Ensure that level is at least 1

            self._create_or_update_subrecord(item, subrecord_count, generation)

        except as_exceptions.RecordNotFound as e:
            try:
                # If this fails for a reason other than it already existing, error out
                self._create_metadata_record()
                userkey_1 = self._make_user_key(1)
                # Create the first subrecord
                self.client.put((self.ns, self.set, userkey_1), {self.subrecord_list_bin: []})
                # Metadata record has just been created. Add the first subrecord
                self._create_or_update_subrecord(item, 1, 1)
            except as_exceptions.RecordExistsError:
                # If the metadata record alread exists, try to insert into the correct subrecord by recursing.
                add_item(self, item)

    def get_all_entries(self, extended_search=False):
        '''
        Get all of the entries from all subrecords flattened and buffered into a list

        Args:
            extended_search(bool): Whether to attempt to fetch records beyond the count of known subrecords
        Returns:
            list: A list of all of the items from all subrecords for this record
            None: If the metadata record does not exist.
        Raises:
            AerospikeError:
                If any of the fetch operations fail.
        '''
        try:
            _, _, bins = self.get_metadata_record()
            min_count = bins[self.subrecourd_count_name]
            keys = []
            for i in range(1, min_count + 1):
                key = (
                    self.ns,
                    self.set,
                    self._make_user_key(i)
                )
                keys.append(key)

            subrecords = self.client.get_many(keys)
            entries = self._get_items_from_subrecords(subrecords)

            # Try to get subrecords beyond the listed amount.
            # It is possible but not guaranteed that they exist.
            if extended_search:
                record_number = min_count + 1
                while True:
                    key = (
                        self.ns,
                        self.set,
                        self._make_user_key(record_number)
                    )
                    try:
                        _, _, bins = self.client.get(key)
                        entries.append(bins)
                    except as_exceptions.RecordNotFound:
                        break
            return entries
        except as_exceptions.RecordNotFound:
            return None

    def _make_user_key(self, record_number):
        '''
        Returns a formatted string to be used as the userkey portion of a key.

        Args:
            record_number (int): Integer >= 1 specifying which subrecord for which to create a key.

        Returns:
            string: A formatted string of the form: 'base-#'
        '''
        return "{}-{}".format(self.base_key, record_number)

    def _create_metadata_record(self):
        '''
        Create the top level information about the key.

        Raises:
            RecordExistsError:
                If the metadata record already exists.
            AerospikeError:
                If the operation fails for any other reason
        '''

        # Only create the metadata record if it does not exist
        policy = {'exists': aerospike.POLICY_EXISTS_CREATE}
        self.client.put(self.metadata_key, {self.subrecourd_count_name: 1})

    def _create_or_update_subrecord(self, item, subrecord_number, generation, retries_remaining=3):
        '''
        Create a new subrecord for the item.
        1. Create or append an item to the given specified subrecord
        2. Update the top level metadata record to mark this subrecord's existence

        If the update causes the specified record to be too large, the operation
        is retried with a new subrecord number

        Args:
            item: The item to be inserted into a subrecord
            subrecord_number (int): An integer >= 1 indicating which subrecord to insert into.
            generation (int): The generation of the metadata record.
            retries_remaining (int): Number of retries remaining for an error caused by inserting
                a record which by itself is greater than the write block size.
        Raises:
            Subclass of AerospikeError if an operation fails for a reason
            other than the update causing the record to be too large.
        '''
        subrecord_userkey = self._make_user_key(subrecord_number)
        subrecord_record_key = (self.ns, self.set, subrecord_userkey)
        try:
            self.client.list_append(
                subrecord_record_key, self.subrecord_list_bin, item)
        except as_exceptions.RecordTooBig as e:
            if retries_remaining == 0:
                raise e
            # The insert overflowed the size capacity of the record, increment the top level record metadata.
            self._update_metadata_record(generation)
            self._create_or_update_subrecord(item, subrecord_number + 1, generation, retries_remaining=(retries_remaining - 1))

    def _update_metadata_record(self, generation):
        '''
        Increment the metadata record's count of subrecords. This is only safe to do if the generation of the metadata matches
        the expected value. Ignore if this fails.
        '''
        update_policy = {'gen': aerospike.POLICY_GEN_EQ}
        meta = {'gen': generation}
        try:
            self.client.increment(self.metadata_key, self.subrecourd_count_name, 1, meta=meta, policy=update_policy)
        except as_exceptions.RecordTooBig:
            raise ASMetadataRecordTooLarge
        except as_exceptions.RecordGenerationError:
            # This means that somebody else has updated the record count already. Don't risk updating again.
            pass

    def _get_items_from_subrecords(self, subrecords):
        '''
        Extract only the items from a list of subrecord tuples
        given records = [
            (key, meta, {'items': [1, 2, ,3]}),
            (key, meta, {'items': [4, 5, 6]}),
            (key, meta, {'items': 7, 8, 9}))
        ]
        returns [1, 2, 3, 4, 5, 6, 7, 8, 9]

        Args:
            subrecords (list<(k, m, b)>): A list of record tuples.

        Returns:
            list: A flattened list of items. Containing all items from the subrecords.
        '''
        entries = []
        # If a subrecord was included in the header of the top level record, but the matching subrecord
        # was not found, ignore it.
        for _, _, sr_bins in subrecords:
            if sr_bins:
                entries.extend(sr_bins[self.subrecord_list_bin])
        return entries

    def subrecord_iterator(self):

        _, _, bins = self.get_metadata_record()
        count = bins[self.subrecourd_count_name]

        def sr_iter():
            '''
            Generator which fetches one subrecord at a time, and yields it to caller
            '''
            for i in range(1, count + 1):
                key = (
                    self.ns,
                    self.set,
                    self._make_user_key(i)
                )
                try:
                    yield self.client.get(key)
                except as_exceptions.RecordNotFound:
                    continue

        # Instantiate the generator and return it.
        return sr_iter()


def main():
    '''
    Simple tests demonstrating the functionality.
    If the database is set up with a small enough write block-size, several subrecords
    will be created.
    '''

    optparser = argparse.ArgumentParser()

    optparser.add_argument(
        "--host", type=str, default="127.0.0.1", metavar="<ADDRESS>",
        help="Address of Aerospike server.")

    optparser.add_argument(
        "--port", type=int, default=3000, metavar="<PORT>",
        help="Port of the Aerospike server.")

    optparser.add_argument(
        "--namespace", type=str, default="test", metavar="<NS>",
        help="Namespace to use for this example")

    optparser.add_argument(
        "-s", "--set", type=str, default="demo", metavar="<SET>",
        help="Set to use for this example")

    optparser.add_argument(
        "-i", "--items", type=int, default=1000, metavar="<ITEMS>",
        help="Number of items to store into the big list")

    options = optparser.parse_args()
    print(options)

    client = aerospike.client({'hosts': [('localhost', 3000)]}).connect()
    ldt = ClientSideBigList(client, 'person1_friends')

    for i in range(options.items):
        # Store a reasonably large item
        ldt.add_item('friend{}'.format(i) * 100)

    print("Stored {} items".format(options.items))

    items = ldt.get_all_entries()
    _, _, bins = ldt.get_metadata_record()
    print(bins)
    print("Known subrecord count is: {}".format(bins['sr_count']))
    print("Fetched {} items:".format(len(items)))

    count = 0
    for sr in ldt.subrecord_iterator():
        if sr:
            count = count + 1

    print("Records yielded: {}".format(count))


if __name__ == '__main__':
    main()
