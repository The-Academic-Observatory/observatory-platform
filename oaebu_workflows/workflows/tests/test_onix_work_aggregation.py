# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Tuan Chien


import unittest

from oaebu_workflows.workflows.onix_work_aggregation import (
    BookWork,
    BookWorkAggregator,
    BookWorkFamily,
    BookWorkFamilyAggregator,
    UnionFind,
    get_pref_product_id,
)


class TestUnionFind(unittest.TestCase):
    """
    Test the UnionFind class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_root_trivial(self):
        n = 5
        uf = UnionFind(n)

        for i in range(n):
            self.assertEqual(uf.root(i), i)

    def test_unite_two(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)

        self.assertEqual(uf.root(0), uf.root(1))

    def test_unite_two_parts(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)
        uf.unite(2, 3)

        self.assertEqual(uf.root(0), uf.root(1))
        self.assertEqual(uf.root(2), uf.root(3))
        self.assertNotEqual(uf.root(0), uf.root(2))

    def test_unite_two_parts_merge(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)
        uf.unite(2, 3)
        uf.unite(0, 2)

        self.assertEqual(uf.root(0), uf.root(1))
        self.assertEqual(uf.root(2), uf.root(3))

        for i in range(4):
            self.assertEqual(uf.root(0), uf.root(i))

    def test_find_trivial(self):
        n = 5
        uf = UnionFind(n)

        for i in range(n - 1):
            self.assertFalse(uf.find(i, i + 1))

    def test_find_after_merge(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)

        self.assertTrue(uf.find(0, 1))
        for i in range(1, n - 1):
            self.assertFalse(uf.find(i, i + 1))

    def test_find_after_two_parts_merge(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)
        uf.unite(2, 3)
        uf.unite(0, 2)

        self.assertFalse(uf.find(3, 4))
        self.assertTrue(uf.find(0, 3))

    def test_get_partition_trivial(self):
        n = 5
        uf = UnionFind(n)
        partition = uf.get_partition()

        self.assertEqual(len(partition), n)

        for i in range(n):
            self.assertEqual(partition[i][0], i)

    def test_get_partition_two_parts(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)
        uf.unite(2, 3)

        partition = uf.get_partition()
        self.assertEqual(len(partition), 3)
        self.assertEqual(partition[0][0], 0)
        self.assertEqual(partition[0][1], 1)
        self.assertEqual(partition[1][0], 2)
        self.assertEqual(partition[1][1], 3)
        self.assertEqual(partition[2][0], 4)

    def test_get_partition_two_parts_merge(self):
        n = 5
        uf = UnionFind(n)
        uf.unite(0, 1)
        uf.unite(2, 3)
        uf.unite(0, 2)

        partition = uf.get_partition()
        self.assertEqual(len(partition), 2)
        self.assertEqual(partition[1][0], 4)

        for i in range(4):
            self.assertEqual(partition[0][i], i)


class TestBookWork(unittest.TestCase):
    """
    Test the BookWork class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_ctor(self):
        work = BookWork(work_id="testid", work_id_type="wid_type", products=[{"ISBN13": "123"}])

        self.assertEqual(work.work_id, "testid")
        self.assertEqual(work.work_id_type, "wid_type")
        self.assertEqual(len(work.products), 1)
        self.assertEqual(len(work.isbns), 1)
        self.assertTrue("123" in work.isbns)

    def test_add_product(self):
        work = BookWork(work_id="testid", work_id_type="wid_type", products=[{"ISBN13": "123"}])

        work.add_product({"ISBN13": "456"})
        self.assertEqual(len(work.products), 2)
        self.assertEqual(len(work.isbns), 2)

        self.assertEqual(work.products[1]["ISBN13"], "456")
        self.assertTrue("456" in work.isbns)

        work.add_product({"ISBN13": "456"})
        self.assertEqual(len(work.products), 2)
        self.assertEqual(len(work.isbns), 2)


class TestBookWorkFamily(unittest.TestCase):
    """
    Test the BookWorkFamily class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_ctor(self):
        work = BookWork(work_id="testid", work_id_type="wid_type", products=[{"ISBN13": "123"}])

        wfam = BookWorkFamily(works=[work])
        self.assertEqual(wfam.work_family_id, None)
        self.assertEqual(wfam.work_family_id_type, None)

        self.assertEqual(len(wfam.works), 1)
        self.assertEqual(wfam.works[0].work_id, "testid")


class TestGetPrefProductId(unittest.TestCase):
    """
    Test the get_pref_product_id function.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_unknown(self):
        relprod = {"unknown": "123"}
        self.assertRaises(Exception, get_pref_product_id, relprod)

    def test_get_pidproprietary(self):
        relprod = {"PID_Proprietary": "pidpro"}
        ptype, pid = get_pref_product_id(relprod)
        self.assertEqual(ptype, "PID_Proprietary")
        self.assertEqual(pid, "pidpro")

    def test_get_doi(self):
        relprod = {"PID_Proprietary": "pidpro", "DOI": "doi"}
        ptype, pid = get_pref_product_id(relprod)
        self.assertEqual(ptype, "DOI")
        self.assertEqual(pid, "doi")

    def test_get_gtin13(self):
        relprod = {"PID_Proprietary": "pidpro", "DOI": "doi", "GTIN_13": "gtin"}
        ptype, pid = get_pref_product_id(relprod)
        self.assertEqual(ptype, "GTIN_13")
        self.assertEqual(pid, "gtin")

    def test_get_isbn(self):
        relprod = {"PID_Proprietary": "pidpro", "DOI": "doi", "GTIN_13": "gtin", "ISBN13": "isbn"}
        ptype, pid = get_pref_product_id(relprod)
        self.assertEqual(ptype, "ISBN13")
        self.assertEqual(pid, "isbn")


class TestGetPrefWorkId(unittest.TestCase):
    """
    Test the get_pref_work_id function.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agg = BookWorkAggregator([])

    def test_unknown(self):
        ids = [{"WorkIDType": "unknown", "IDValue": "value"}]
        wtype, wid = self.agg.get_pref_work_id(ids)

        self.assertEqual(wtype, "unknown")
        self.assertEqual(wid, "value")

    def test_pid_proprietary(self):
        ids = [{"WorkIDType": "unknown", "IDValue": "value"}, {"WorkIDType": "Proprietary", "IDValue": "pidpro"}]

        wtype, wid = self.agg.get_pref_work_id(ids)
        self.assertEqual(wtype, "Proprietary")
        self.assertEqual(wid, "pidpro")

    def test_doi(self):
        ids = [
            {"WorkIDType": "unknown", "IDValue": "value"},
            {"WorkIDType": "PID_Proprietary", "IDValue": "pidpro"},
            {"WorkIDType": "DOI", "IDValue": "doi"},
        ]

        wtype, wid = self.agg.get_pref_work_id(ids)
        self.assertEqual(wtype, "DOI")
        self.assertEqual(wid, "doi")

    def test_gtin13(self):
        ids = [
            {"WorkIDType": "unknown", "IDValue": "value"},
            {"WorkIDType": "PID_Proprietary", "IDValue": "pidpro"},
            {"WorkIDType": "DOI", "IDValue": "doi"},
            {"WorkIDType": "GTIN_13", "IDValue": "gtin"},
        ]

        wtype, wid = self.agg.get_pref_work_id(ids)
        self.assertEqual(wtype, "GTIN_13")
        self.assertEqual(wid, "gtin")

    def test_isbn13(self):
        ids = [
            {"WorkIDType": "unknown", "IDValue": "value"},
            {"WorkIDType": "PID_Proprietary", "IDValue": "pidpro"},
            {"WorkIDType": "DOI", "IDValue": "doi"},
            {"WorkIDType": "GTIN_13", "IDValue": "gtin"},
            {"WorkIDType": "ISBN-13", "IDValue": "isbn"},
        ]

        wtype, wid = self.agg.get_pref_work_id(ids)
        self.assertEqual(wtype, "ISBN13")
        self.assertEqual(wid, "isbn")


class TestBookWorkAggregator(unittest.TestCase):
    """
    Test the BookWorkAggregator class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_ctor(self):
        agg = BookWorkAggregator([])
        self.assertEqual(len(agg.records), 0)

    def test_is_relevant_worK_relation(self):
        agg = BookWorkAggregator([])
        relwork1 = {"WorkRelationCode": "Manifestation of"}

        self.assertTrue(agg.is_relevant_work_relation(relwork1))

    def test_aggregate_empty(self):
        agg = BookWorkAggregator([])
        works = agg.aggregate()
        self.assertEqual(len(works), 0)

    def test_agg_relworks(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [
                                {"WorkIDType": "ISBN-13", "IDValue": "246"},
                                {"WorkIDType": "GTIN_13", "IDValue": "123"},
                            ],
                        },
                    ],
                },
                {
                    "ISBN13": "246",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "111",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "111"}],
                        },
                        {
                            "WorkRelationCode": "something unknown",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "222",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "222"}],
                        },
                    ],
                },
            ]
        )

        agg.agg_relworks()
        works = agg.get_works_from_partition(agg.uf.get_partition())
        self.assertEqual(len(works), 3)

        work1 = works[0]
        self.assertEqual(len(work1.products), 2)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("246" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.products), 1)
        self.assertTrue("111" in work2.isbns)

        work3 = works[2]
        self.assertEqual(len(work3.products), 1)
        self.assertTrue("222" in work3.isbns)

    def test_agg_relworks_gtin13(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "246",
                    "GTIN_13": "246",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "111",
                    "GTIN_13": "111",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "111"}],
                        },
                        {
                            "WorkRelationCode": "something unknown",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "222",
                    "GTIN_13": "222",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "222"}],
                        },
                    ],
                },
            ]
        )
        agg.agg_relworks()
        works = agg.get_works_from_partition(agg.uf.get_partition())
        self.assertEqual(len(works), 3)

        work1 = works[0]
        self.assertEqual(len(work1.products), 2)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("246" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.products), 1)
        self.assertTrue("111" in work2.isbns)

        work3 = works[2]
        self.assertEqual(len(work3.products), 1)
        self.assertTrue("222" in work3.isbns)

    def test_agg_relworks_pid_proprietary(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "PID_Proprietary": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "PID_Proprietary", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "246",
                    "GTIN_13": "246",
                    "PID_Proprietary": "246",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "PID_Proprietary", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "111",
                    "GTIN_13": "111",
                    "PID_Proprietary": "111",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "PID_Proprietary", "IDValue": "111"}],
                        },
                        {
                            "WorkRelationCode": "something unknown",
                            "WorkIdentifiers": [{"WorkIDType": "PID_Proprietary", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "222",
                    "GTIN_13": "222",
                    "PID_Proprietary": "222",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "PID_Proprietary", "IDValue": "222"}],
                        },
                    ],
                },
            ]
        )
        agg.agg_relworks()
        works = agg.get_works_from_partition(agg.uf.get_partition())
        self.assertEqual(len(works), 3)

        work1 = works[0]
        self.assertEqual(len(work1.products), 2)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("246" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.products), 1)
        self.assertTrue("111" in work2.isbns)

        work3 = works[2]
        self.assertEqual(len(work3.products), 1)
        self.assertTrue("222" in work3.isbns)

    def test_agg_products(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "234"}
                    ],
                },
                {
                    "ISBN13": "234",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
                {
                    "ISBN13": "456",
                    "RelatedProducts": [{"ProductRelationCodes": ["random", "something random"], "ISBN13": "123"}],
                },
                {
                    "ISBN13": "789",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
            ]
        )
        agg.agg_relproducts()
        works = agg.get_works_from_partition(agg.uf.get_partition())
        self.assertEqual(len(works), 2)

        work1 = works[0]
        self.assertEqual(len(work1.isbns), 3)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("234" in work1.isbns)
        self.assertTrue("789" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.isbns), 1)
        self.assertTrue("456" in work2.isbns)

    def test_agg_relprod_doi(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "DOI": "doi",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "DOI": "234"}
                    ],
                },
            ]
        )

        self.assertRaises(Exception, agg.agg_relproducts)

    def test_agg_relprod_gtin13(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "GTIN_13": "234"}
                    ],
                },
                {
                    "ISBN13": "234",
                    "GTIN_13": "234",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "GTIN_13": "123"}
                    ],
                },
                {
                    "ISBN13": "456",
                    "GTIN_13": "456",
                    "RelatedProducts": [{"ProductRelationCodes": ["random", "something random"], "GTIN_13": "123"}],
                },
                {
                    "ISBN13": "789",
                    "GTIN_13": "789",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "GTIN_13": "123"}
                    ],
                },
            ]
        )

        agg.agg_relproducts()
        works = agg.get_works_from_partition(agg.uf.get_partition())
        self.assertEqual(len(works), 2)

        work1 = works[0]
        self.assertEqual(len(work1.isbns), 3)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("234" in work1.isbns)
        self.assertTrue("789" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.isbns), 1)
        self.assertTrue("456" in work2.isbns)

    def test_aggregate1(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "246",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "111",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "111"}],
                        },
                        {
                            "WorkRelationCode": "something unknown",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "222",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "Proprietary", "IDValue": "222"}],
                        },
                    ],
                },
                {
                    "ISBN13": "1010",
                    "PID_Proprietary": "1010",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "Proprietary", "IDValue": "1011"}],
                        },
                    ],
                },
                {
                    "ISBN13": "1011",
                    "PID_Proprietary": "1011",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "Proprietary", "IDValue": "1010"}],
                        },
                    ],
                },
            ]
        )

        works = agg.aggregate()
        self.assertEqual(len(works), 4)

        work1 = works[0]
        self.assertEqual(len(work1.products), 2)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("246" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.products), 1)
        self.assertTrue("111" in work2.isbns)

        work3 = works[2]
        self.assertEqual(len(work3.products), 1)
        self.assertTrue("222" in work3.isbns)

        work4 = works[3]
        self.assertEqual(len(work4.products), 2)
        self.assertTrue("1010" in work4.isbns)
        self.assertTrue("1011" in work4.isbns)

    def test_aggregate2(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "234"}
                    ],
                },
                {
                    "ISBN13": "234",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
                {
                    "ISBN13": "456",
                    "RelatedProducts": [{"ProductRelationCodes": ["random", "something random"], "ISBN13": "123"}],
                },
                {
                    "ISBN13": "789",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
                {
                    "ISBN13": "1010",
                    "PID_Proprietary": "1010",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "PID_Proprietary": "1011"}
                    ],
                },
                {
                    "ISBN13": "1011",
                    "PID_Proprietary": "1011",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "PID_Proprietary": "1010"}
                    ],
                },
            ]
        )

        works = agg.aggregate()
        self.assertEqual(len(works), 3)

        work1 = works[0]
        self.assertEqual(len(work1.isbns), 3)
        self.assertTrue("123" in work1.isbns)
        self.assertTrue("234" in work1.isbns)
        self.assertTrue("789" in work1.isbns)

        work2 = works[1]
        self.assertEqual(len(work2.isbns), 1)
        self.assertTrue("456" in work2.isbns)

        work3 = works[2]
        self.assertEqual(len(work3.isbns), 2)
        self.assertTrue("1010" in work3.isbns)
        self.assertTrue("1011" in work3.isbns)

    def test_aggregate3(self):
        agg = BookWorkAggregator([])
        lookup_table = agg.aggregate()
        self.assertEqual(len(lookup_table), 0)

    def test_agg_works_products_composite(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "111",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [
                                {"WorkIDType": "ISBN-13", "IDValue": "112"},
                            ],
                        },
                    ],
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format"], "ISBN13": "211"},
                    ],
                },
                {
                    "ISBN13": "112",
                    "RelatedWorks": [],
                    "RelatedProducts": [],
                },
                {
                    "ISBN13": "211",
                    "RelatedWorks": [],
                    "RelatedProducts": [],
                },
            ]
        )

        works = agg.aggregate()
        self.assertEqual(len(works), 1)

    def test_get_pid_idx_missing(self):
        agg = BookWorkAggregator([])
        pid_type = "ISBN13"
        pid = "123"
        isbn13_to_index = {}
        gtin13_to_product = {}

        self.assertEqual(agg.get_pid_idx(pid_type, pid), None)

        pid_type = "GTIN_13"
        self.assertEqual(agg.get_pid_idx(pid_type, pid), None)

        pid_type = "PID_Proprietary"
        self.assertEqual(agg.get_pid_idx(pid_type, pid), None)

    def test_get_pid_idx_unknown(self):
        agg = BookWorkAggregator([])
        pid_type = "somethingunknown"
        pid = "123"
        self.assertRaises(Exception, agg.get_pid_idx, pid_type, pid)

    def test_agg_relprod_missing_record(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "DOI": "doi",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "234"}
                    ],
                },
            ]
        )
        works = agg.aggregate()
        self.assertEqual(len(agg.errors), 1)

    def test_get_works_lookup_table(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "234"}
                    ],
                },
                {
                    "ISBN13": "234",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
                {
                    "ISBN13": "456",
                    "RelatedProducts": [{"ProductRelationCodes": ["random", "something random"], "ISBN13": "123"}],
                },
                {
                    "ISBN13": "789",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "123"}
                    ],
                },
            ]
        )

        agg.aggregate()
        lookup_table = agg.get_works_lookup_table()
        self.assertEqual(len(lookup_table), 4)

        lookup_map = {lookup_table[i]["isbn13"]: lookup_table[i]["work_id"] for i in range(len(lookup_table))}
        self.assertEqual(lookup_map["123"], "123")
        self.assertEqual(lookup_map["234"], "123")
        self.assertEqual(lookup_map["789"], "123")
        self.assertEqual(lookup_map["456"], "456")

    def log_agg_related_product_errors(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "RelatedProducts": [
                        {"ProductRelationCodes": ["Alternative format", "something random"], "ISBN13": "234"}
                    ],
                },
            ]
        )

        agg.aggregate()
        self.assertEqual(
            agg.errors[0],
            "Product 123 has a related product ISBN13:234 of types ['Alternative format', 'something random'] missing a product record.",
        )

    def test_log_agg_relworks_errors(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
            ]
        )

        agg.aggregate()
        self.assertEqual(
            agg.errors[0],
            "Product ISBN13:123 is a manifestation of ISBN13:246, which is not given as a product identifier in any ONIX product record.",
        )

    def test_log_agg_relworks_errors_miss_gtin(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "GTIN_13", "IDValue": "246"}],
                        },
                    ],
                },
            ]
        )

        agg.aggregate()
        self.assertEqual(
            agg.errors[0],
            "Product ISBN13:123 is a manifestation of GTIN_13:246, which is not given as a product identifier in any ONIX product record.",
        )

    def test_log_get_works_lookup_table_errors(self):
        agg = BookWorkAggregator(
            [
                {
                    "ISBN13": "123",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "789"}],
                        },
                    ],
                },
                {
                    "ISBN13": "246",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "246"}],
                        },
                    ],
                },
                {
                    "ISBN13": "789",
                    "GTIN_13": "123",
                    "RelatedWorks": [
                        {
                            "WorkRelationCode": "Manifestation of",
                            "WorkIdentifiers": [{"WorkIDType": "ISBN-13", "IDValue": "789"}],
                        },
                    ],
                },
            ]
        )

        agg.aggregate()
        table = agg.get_works_lookup_table()
        self.assertEqual(agg.errors[0][0:54], "Warning: product 123 has multiple work ID assignments:")


class TestBookWorkFamilyAggregator(unittest.TestCase):
    """
    Test the BookWorkFamilyAggregator class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_ctor(self):
        works = [
            BookWork(work_id="1", work_id_type="test", products=[{"ISBN13": "123"}]),
        ]

        agg = BookWorkFamilyAggregator(works)
        self.assertEqual(len(agg.works), 1)

    def test_agg_relproducts(self):
        products1 = [
            {
                "ISBN13": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "456"},
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "789"},
                ],
            },
            {"ISBN13": "456", "RelatedProducts": []},
            {"ISBN13": "147", "RelatedProducts": []},
        ]

        products2 = [
            {
                "ISBN13": "258",
                "RelatedProducts": [
                    {
                        "ProductRelationCodes": ["Is later edition of first edition", "something random"],
                        "ISBN13": "147",
                    },
                ],
            },
        ]

        products3 = [
            {
                "ISBN13": "369",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["something random"], "ISBN13": "123"},
                ],
            },
        ]

        products4 = [
            {
                "ISBN13": "789",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaced by", "something random"], "ISBN13": "456"},
                ],
            },
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
            BookWork(work_id="2", work_id_type="test", products=products2),
            BookWork(work_id="3", work_id_type="test", products=products3),
            BookWork(work_id="4", work_id_type="test", products=products4),
        ]

        agg = BookWorkFamilyAggregator(works)
        wfam = agg.agg_relproducts()
        self.assertEqual(len(wfam), 2)

        self.assertEqual(wfam[0].work_family_id, "1")
        self.assertEqual(wfam[0].work_family_id_type, "test")
        self.assertEqual(len(wfam[0].works), 3)
        self.assertEqual(wfam[0].works[0].work_id, "1")
        self.assertEqual(wfam[0].works[1].work_id, "2")
        self.assertEqual(wfam[0].works[2].work_id, "4")

        self.assertEqual(wfam[1].work_family_id, "3")
        self.assertEqual(wfam[1].work_family_id_type, "test")
        self.assertEqual(len(wfam[1].works), 1)
        self.assertEqual(wfam[1].works[0].work_id, "3")

    def test_aggregate_products_missing(self):
        products1 = [
            {
                "ISBN13": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "456"},
                ],
            }
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
        ]

        agg = BookWorkFamilyAggregator(works)
        wfam = agg.aggregate()
        self.assertEqual(len(wfam), 1)
        self.assertTrue("123" in wfam[0].works[0].isbns)

    def test_aggregate_products(self):
        products1 = [
            {
                "ISBN13": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "456"},
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "789"},
                ],
            },
            {"ISBN13": "456", "RelatedProducts": []},
            {"ISBN13": "147", "RelatedProducts": []},
        ]

        products2 = [
            {
                "ISBN13": "258",
                "RelatedProducts": [
                    {
                        "ProductRelationCodes": ["Is later edition of first edition", "something random"],
                        "ISBN13": "147",
                    },
                ],
            },
        ]

        products3 = [
            {
                "ISBN13": "369",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["something random"], "ISBN13": "123"},
                ],
            },
        ]

        products4 = [
            {
                "ISBN13": "789",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaced by", "something random"], "ISBN13": "456"},
                ],
            },
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
            BookWork(work_id="2", work_id_type="test", products=products2),
            BookWork(work_id="3", work_id_type="test", products=products3),
            BookWork(work_id="4", work_id_type="test", products=products4),
        ]

        agg = BookWorkFamilyAggregator(works)
        wfam = agg.aggregate()
        self.assertEqual(len(wfam), 2)

        self.assertEqual(wfam[0].work_family_id, "1")
        self.assertEqual(wfam[0].works[0].work_id, "1")
        self.assertEqual(wfam[0].works[1].work_id, "2")
        self.assertEqual(wfam[0].works[2].work_id, "4")

        self.assertEqual(wfam[1].work_family_id, "3")
        self.assertEqual(wfam[1].works[0].work_id, "3")

    def test_aggregate_products_gtin13(self):
        products1 = [
            {
                "ISBN13": "123",
                "GTIN_13": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "GTIN_13": "456"},
                    {"ProductRelationCodes": ["Replaces", "something random"], "GTIN_13": "789"},
                ],
            },
            {"ISBN13": "456", "GTIN_13": "456", "RelatedProducts": []},
            {"ISBN13": "147", "GTIN_13": "147", "RelatedProducts": []},
        ]

        products2 = [
            {
                "ISBN13": "258",
                "GTIN_13": "258",
                "RelatedProducts": [
                    {
                        "ProductRelationCodes": ["Is later edition of first edition", "something random"],
                        "GTIN_13": "147",
                    },
                ],
            },
        ]

        products3 = [
            {
                "ISBN13": "369",
                "GTIN_13": "369",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["something random"], "GTIN_13": "123"},
                ],
            },
        ]

        products4 = [
            {
                "ISBN13": "789",
                "GTIN_13": "789",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaced by", "something random"], "GTIN_13": "456"},
                ],
            },
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
            BookWork(work_id="2", work_id_type="test", products=products2),
            BookWork(work_id="3", work_id_type="test", products=products3),
            BookWork(work_id="4", work_id_type="test", products=products4),
        ]

        agg = BookWorkFamilyAggregator(works)
        wfam = agg.aggregate()
        self.assertEqual(len(wfam), 2)

        self.assertEqual(wfam[0].work_family_id, "1")
        self.assertEqual(wfam[0].works[0].work_id, "1")
        self.assertEqual(wfam[0].works[1].work_id, "2")
        self.assertEqual(wfam[0].works[2].work_id, "4")

        self.assertEqual(wfam[1].work_family_id, "3")
        self.assertEqual(wfam[1].works[0].work_id, "3")

    def test_aggregate_products_pid_proprietary(self):
        products1 = [
            {
                "ISBN13": "123",
                "GTIN_13": "123",
                "PID_Proprietary": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "PID_Proprietary": "456"},
                    {"ProductRelationCodes": ["Replaces", "something random"], "PID_Proprietary": "789"},
                ],
            },
            {"ISBN13": "456", "GTIN_13": "456", "PID_Proprietary": "456", "RelatedProducts": []},
            {"ISBN13": "147", "GTIN_13": "147", "PID_Proprietary": "147", "RelatedProducts": []},
        ]

        products2 = [
            {
                "ISBN13": "258",
                "GTIN_13": "258",
                "PID_Proprietary": "258",
                "RelatedProducts": [
                    {
                        "ProductRelationCodes": ["Is later edition of first edition", "something random"],
                        "PID_Proprietary": "147",
                    },
                ],
            },
        ]

        products3 = [
            {
                "ISBN13": "369",
                "GTIN_13": "369",
                "PID_Proprietary": "369",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["something random"], "PID_Proprietary": "123"},
                ],
            },
        ]

        products4 = [
            {
                "ISBN13": "789",
                "GTIN_13": "789",
                "PID_Proprietary": "789",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaced by", "something random"], "PID_Proprietary": "456"},
                ],
            },
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
            BookWork(work_id="2", work_id_type="test", products=products2),
            BookWork(work_id="3", work_id_type="test", products=products3),
            BookWork(work_id="4", work_id_type="test", products=products4),
        ]

        agg = BookWorkFamilyAggregator(works)
        wfam = agg.aggregate()
        self.assertEqual(len(wfam), 2)

        self.assertEqual(wfam[0].work_family_id, "1")
        self.assertEqual(wfam[0].works[0].work_id, "1")
        self.assertEqual(wfam[0].works[1].work_id, "2")
        self.assertEqual(wfam[0].works[2].work_id, "4")

        self.assertEqual(wfam[1].work_family_id, "3")
        self.assertEqual(wfam[1].works[0].work_id, "3")

    def test_get_wid_unsupported(self):
        products1 = [
            {
                "ISBN13": "123",
                "xid": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "xid": "456"},
                ],
            }
        ]

        works = [BookWork(work_id="1", work_id_type="test", products=products1)]
        agg = BookWorkFamilyAggregator(works)
        self.assertRaises(Exception, agg.aggregate)

    def test_get_wid_idx_unsupported(self):
        works = [BookWork(work_id="1", work_id_type="test", products=[])]
        agg = BookWorkFamilyAggregator(works)
        self.assertRaises(Exception, agg.get_wid_idx, "unknown", "123", {}, {}, {})

    def test_get_wid_idx_missing_isbn(self):
        works = [BookWork(work_id="1", work_id_type="test", products=[])]
        agg = BookWorkFamilyAggregator(works)
        pid_type = "ISBN13"
        pid = "123"
        isbn_table = {}
        gtin_table = {}
        proprietary_table = {}
        self.assertEqual(agg.get_wid_idx(pid_type, pid, isbn_table, gtin_table, proprietary_table), None)

    def test_get_wid_idx_missing_gtin(self):
        works = [BookWork(work_id="1", work_id_type="test", products=[])]
        agg = BookWorkFamilyAggregator(works)
        pid_type = "GTIN_13"
        pid = "123"
        isbn_table = {}
        gtin_table = {}
        proprietary_table = {}
        self.assertEqual(agg.get_wid_idx(pid_type, pid, isbn_table, gtin_table, proprietary_table), None)

    def test_get_wid_idx_missing_pid_proprietary(self):
        works = [BookWork(work_id="1", work_id_type="test", products=[])]
        agg = BookWorkFamilyAggregator(works)
        pid_type = "PID_Proprietary"
        pid = "123"
        isbn_table = {}
        gtin_table = {}
        proprietary_table = {}
        self.assertEqual(agg.get_wid_idx(pid_type, pid, isbn_table, gtin_table, proprietary_table), None)

    def test_get_works_family_lookup_table(self):
        products1 = [
            {
                "ISBN13": "123",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "456"},
                    {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "789"},
                ],
            },
            {"ISBN13": "456", "RelatedProducts": []},
            {"ISBN13": "147", "RelatedProducts": []},
        ]

        products2 = [
            {
                "ISBN13": "258",
                "RelatedProducts": [
                    {
                        "ProductRelationCodes": ["Is later edition of first edition", "something random"],
                        "ISBN13": "147",
                    },
                ],
            },
        ]

        products3 = [
            {
                "ISBN13": "369",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["something random"], "ISBN13": "123"},
                ],
            },
        ]

        products4 = [
            {
                "ISBN13": "789",
                "RelatedProducts": [
                    {"ProductRelationCodes": ["Replaced by", "something random"], "ISBN13": "456"},
                ],
            },
        ]

        works = [
            BookWork(work_id="1", work_id_type="test", products=products1),
            BookWork(work_id="2", work_id_type="test", products=products2),
            BookWork(work_id="3", work_id_type="test", products=products3),
            BookWork(work_id="4", work_id_type="test", products=products4),
        ]

        agg = BookWorkFamilyAggregator(works)
        agg.aggregate()
        lookup_table = agg.get_works_family_lookup_table()
        self.assertEqual(len(lookup_table), 6)

        lookup_map = {lookup_table[i]["isbn13"]: lookup_table[i]["work_family_id"] for i in range(len(lookup_table))}

        self.assertEqual(lookup_map["123"], "1")
        self.assertEqual(lookup_map["456"], "1")
        self.assertEqual(lookup_map["147"], "1")
        self.assertEqual(lookup_map["258"], "1")
        self.assertEqual(lookup_map["789"], "1")
        self.assertEqual(lookup_map["369"], "3")
