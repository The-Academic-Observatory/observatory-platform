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


from typing import Dict, List, Set, Tuple, Union

import numpy as np


class UnionFind:
    """
    Union Find using weighted quick union and path compression.
    Instead of working on objects and requiring further implementation of comparators and size methods,
    this will operate on integers. Users should handle the mapping from objects to a distinct integer, e.g., by using
    the index of the objects if they are in a list.
    See: https://en.wikipedia.org/wiki/Disjoint-set_data_structure
    and https://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf
    """

    def __init__(self, size: int):
        """
        :param size: Number of elements we are dealing with in total.
        """

        self.id = np.arange(size)  # Root mapping.
        self.tree_sizes = [1] * size

    def root(self, node: int) -> int:
        """
        Find the root (representative) of an object.  Update root mappings along the way (path compression).
        :param node: Object to find the root for.
        :return: The object's root representative.
        """

        while node != self.id[node]:
            self.id[node] = self.id[self.id[node]]
            node = self.id[node]

        return node

    def find(self, p: int, q: int) -> bool:
        """
        Check if two objects have the same root representative.
        :param p: First object.
        :param q: Second object.
        :return: Whether p, q have the same root representative.
        """

        return self.root(p) == self.root(q)

    def unite(self, p: int, q: int):
        """
        Merge two objects into the same class, i.e., make the two objects have the same root representative.
        Use weighted union to merge smaller trees into the bigger trees.
        :param p First object.
        :param q Second object.
        """

        i = self.root(p)
        j = self.root(q)

        if self.find(p, q):
            return

        if self.tree_sizes[i] < self.tree_sizes[j]:
            self.id[i] = j
            self.tree_sizes[j] + self.tree_sizes[i]
        else:
            self.id[j] = i
            self.tree_sizes[i] += self.tree_sizes[j]

    def get_partition(self) -> List[List[int]]:
        """
        Get the current class partition of the objects.
        :return: Partition of the objects as a list of lists (no guaranteed ordering).
        """

        n = len(self.id)

        partition = {}
        for i in range(n):
            root = self.root(i)
            if root not in partition:
                partition[root] = [i]
            else:
                partition[root].append(i)

        return list(partition.values())


class BookWork:
    """
    A book work is an abstract entity comprising the intellectual property embodied in a manifestation.
    Works manifest themselves as products of different form, e.g., paperback, PDF.
    """

    def __init__(self, *, work_id: str, work_id_type: str, products: List[Dict]):
        """
        :param work_id: The Work ID.
        :param work_id_type: Type scheme used in the Work ID.
        :param products: List of products that manifest the work.
        """

        self.work_id = work_id
        self.work_id_type = work_id_type
        self.products = products
        self.isbns = set([product["ISBN13"] for product in products])

    def add_product(self, product: dict):
        """
        Add a product to the work.
        :param product: A product record.
        """

        isbn = product["ISBN13"]
        if isbn in self.isbns:
            return

        self.products.append(product)
        self.isbns.add(isbn)


class BookWorkFamily:
    """
    A book work family aggregates different editions of a work together.
    """

    def __init__(self, *, works: List[BookWork], work_family_id=None, work_family_id_type: Union[None, str] = None):
        """
        :param works: List of works in the family.
        :param work_family_id: Work family ID.
        :param work_family_id_type: Type of Work family ID.
        """

        self.works = works
        self.work_family_id = work_family_id
        self.work_family_id_type = work_family_id_type
        self.isbns = set()


def get_pref_product_id(relprod: dict) -> Tuple[str, str]:
    """
    Get the most preferred product ID. It will return the one with the lowest preference
    number in the id_pref list, or an arbitrary identifier from the list if the listed preferences are not found.
    :param relprod: Related product record.
    :return: The Product ID type, and Product ID as a pair.
    """

    if "ISBN13" in relprod and relprod["ISBN13"] is not None:
        return "ISBN13", relprod["ISBN13"]

    if "GTIN_13" in relprod and relprod["GTIN_13"] is not None:
        return "GTIN_13", relprod["GTIN_13"]

    if "DOI" in relprod and relprod["DOI"] is not None:
        return "DOI", relprod["DOI"]

    if "PID_Proprietary" in relprod and relprod["PID_Proprietary"] is not None:
        return "PID_Proprietary", relprod["PID_Proprietary"]

    raise Exception(f"Unhandled product identifier found in {relprod}")


class BookWorkAggregator:
    """
    Aggregates ONIX records into "works" (BookWork object). If the WorkID exists in the ONIX record, it will use that.
    The order of preference for the work identifier types is: ISBN-13 > DOI > Proprietary > everything else.
    If no identifier is specified in ONIX, i.e., no RelatedWorks info, one of the ISBN13 in the work will be used as a
    representative ID.
    """

    def __init__(self, records: List[dict]):
        """
        :param records: List of ONIX Product records.
        """

        self.records = records
        self.relevant_product_codes = self.set_relevant_product_relation_codes()
        self.errors = list()
        self.works = list()

        self.n = len(self.records)
        self.isbn13_to_index = {self.records[i]["ISBN13"]: i for i in range(self.n)}

        self.gtin13_to_product = {}
        for record in self.records:
            if "GTIN_13" in record and record["GTIN_13"] is not None:
                gtin = record["GTIN_13"]
                self.gtin13_to_product[gtin] = record

        self.proprietary_to_product = {}
        for record in self.records:
            if "PID_Proprietary" in record and record["PID_Proprietary"] is not None:
                pid_proprietary = record["PID_Proprietary"]
                self.proprietary_to_product[pid_proprietary] = record

        self.uf = UnionFind(self.n)

    def get_pref_work_id(self, identifiers: List[Dict]) -> Tuple[Union[None, str], Union[None, str]]:
        """
        Tries to map the work identifier back to ISBN.
        :param identifiers: List of WorkIdentifiers.
        :return: Preferred work id type and the work id. None represents unknoown work_id type.
        """

        id_pref = {"ISBN-13": 0, "GTIN_13": 1, "DOI": 2, "Proprietary": 3}

        work_id = None
        work_id_type = None
        best_pref = float("inf")
        for wid in identifiers:
            wid_type = wid["WorkIDType"]
            pref = len(id_pref)

            if wid_type in id_pref:
                pref = id_pref[wid_type]

            if pref < best_pref:
                best_pref = pref
                work_id = wid["IDValue"]
                work_id_type = wid_type

        if work_id_type == "ISBN-13":
            return "ISBN13", work_id

        if work_id_type == "GTIN_13" and work_id in self.gtin13_to_product:
            product = self.gtin13_to_product[work_id]
            return "ISBN13", product["ISBN13"]

        if work_id_type == "Proprietary" and work_id in self.proprietary_to_product:
            product = self.proprietary_to_product[work_id]
            return "ISBN13", product["ISBN13"]

        return work_id_type, work_id

    def aggregate(self) -> List[BookWork]:
        """
        Run the aggregation process.
        Separate out the records into those containing RelatedWorks info, RelatedProducts info, and neithe.
        For a single publisher, this should only ever be 1 of the cases.
        Run different aggregation procedures for the 3 cases.
        If publishers are doing something funky, then we need to revisit this procedure.
        :return: List of BookWork objects representing the aggregated product records.
        """

        if self.n == 0:
            return self.works

        self.agg_relworks()
        self.agg_relproducts()
        self.works = self.get_works_from_partition(self.uf.get_partition())

        return self.works

    def is_relevant_work_relation(self, relwork: dict) -> bool:
        """
        Check if the work relation code indicates a manifestation.
        :param relwork: Related work.
        :return: Whether the related work is a manifestation of the current work.
        """

        code = relwork["WorkRelationCode"]
        if code == "Manifestation of":
            return True

        return False

    def log_agg_relworks_errors(self, pisbn: str, wtype: str, wid: str):
        """Log any errors from aggregating along RelatedWorks.
        :param pisbn: The product's ISBN.
        :param wtype: Type of WorkID.
        :param wid: WorkID of the related work.
        :return: True if we logged an error, False if it was OK.
        """

        if wid not in self.isbn13_to_index:
            error_msg = f"Product ISBN13:{pisbn} is a manifestation of {wtype}:{wid}, which is not given as a product identifier in any ONIX product record."
            self.errors.append(error_msg)
            return True

        return False

    def agg_relworks(self) -> List[BookWork]:
        """
        Collect the entries with "Manifestation of" relation codes into a single work.  The Work ID from that field will be used.  This assumes that every product
        record has a "Manifestation of" field entry that either points to itself or
        something else. Revisit this if a publisher does it differently.
        :return: List of BookWork objects that categorise the product records.
        """

        for record in self.records:
            if "RelatedWorks" not in record:
                continue

            isbn = record["ISBN13"]
            current_idx = self.isbn13_to_index[isbn]
            manifestations = set()

            for relwork in record["RelatedWorks"]:
                if not self.is_relevant_work_relation(relwork):
                    continue

                wtype, wid = self.get_pref_work_id(relwork["WorkIdentifiers"])
                if self.log_agg_relworks_errors(isbn, wtype, wid):
                    continue

                manifestations.add(wid)
                wid_idx = self.isbn13_to_index[wid]
                self.uf.unite(current_idx, wid_idx)

            if len(manifestations) > 1:
                self.log_get_works_lookup_table_errors(manifestations, isbn)

    def get_pid_idx(self, pid_type: str, pid: str) -> Union[None, int]:
        """
        Get the product index (in self.records) using the Product ID information.
        :param pid_type: Product ID type.
        :param pid: Product ID.
        :return: Index to the product or None if record doesn't exist.
        """

        if pid_type == "ISBN13":
            isbn = pid
        elif pid_type == "GTIN_13":
            if pid not in self.gtin13_to_product:
                return None
            relrec = self.gtin13_to_product[pid]
            isbn = relrec["ISBN13"]
        elif pid_type == "PID_Proprietary":
            if pid not in self.proprietary_to_product:
                return None
            relrec = self.proprietary_to_product[pid]
            isbn = relrec["ISBN13"]
        else:
            raise Exception(f"No handling implemented for {pid_type}")

        if isbn not in self.isbn13_to_index:
            return None

        return self.isbn13_to_index[isbn]

    def set_relevant_product_relation_codes(self) -> Set[str]:
        """
        :return: Set of relevant product codes indicating a manifestation of the current work.
        """

        return set(
            [
                "Alternative format",
                "Is remaindered as",
                "Is remainder of",
                "Epublication based on (print product)",
                "POD replacement for",
                "Replaced by POD",
                "Electronic version available as",
                "Is signed version of",
                "Has signed version",
            ]
        )

    def is_relevant_product_relation(self, relprod: dict) -> bool:
        """
        Check if the related product has a relation indicating it's the same work as the current work.
        :param relprod: Related product.
        :return: Whether the product is a manifestation.
        """

        codes = set(relprod["ProductRelationCodes"])
        for relcode in self.relevant_product_codes:
            if relcode in codes:
                return True

        return False

    def log_agg_related_product_errors(self, pisbn: str, relation: str, ptype: str, pid: str):
        """
        Log the errors from aggregating along RelatedProducts.

        :param pisbn: The product's ISBN.
        :param relation: The relation code.
        :param ptype: Related product's identifer type.
        :param pid: Related product's identifier.
        """

        error_msg = f"Product ISBN13:{pisbn} has a related product {ptype}:{pid} of types {relation} which is not given as a product identifier in any ONIX product record."
        self.errors.append(error_msg)

    def get_works_from_partition(self, partition: List[List[int]]) -> List[BookWork]:
        """
        Convert the partition of equivalence classes of record indices into equivalence classes of BookWork objects.
        :param partition: Partition of product record indices as works equivalence classes.
        :return: Partition as BookWork objects.
        """

        n = len(self.records)
        index_to_product = {i: self.records[i] for i in range(n)}

        works = list()
        for part in partition:
            products = [index_to_product[i] for i in part]
            # First ISBN is the WorkID representative
            work_id = products[0]["ISBN13"]
            works.append(BookWork(work_id=work_id, work_id_type="ISBN13", products=products))

        return works

    def agg_relproducts(self) -> List[BookWork]:
        """
        Aggregate the entries with targeted relation codes into a single work.
        Currently this is:
            "Alternative format".
        The Work ID will be an arbitrary ISBN13 representative from each work.
        :return: List of BookWork objects that categorise the product records.
        """

        for record in self.records:
            if "RelatedProducts" not in record:
                continue

            isbn = record["ISBN13"]
            current_idx = self.isbn13_to_index[isbn]

            for relprod in record["RelatedProducts"]:
                if not self.is_relevant_product_relation(relprod):
                    continue

                pid_type, pid = get_pref_product_id(relprod)
                pid_idx = self.get_pid_idx(pid_type, pid)

                if pid_idx is None:
                    relation = relprod["ProductRelationCodes"]
                    self.log_agg_related_product_errors(isbn, relation, pid_type, pid)
                    continue

                self.uf.unite(current_idx, pid_idx)

    def log_get_works_lookup_table_errors(self, manifestations: Set[str], isbn: str):
        """Log an error when an ISBN is assigned to multiple WorkIDs.

        :param manifestations: List of work IDs manifesting this product manifests.
        :param isbn: ISBN that has multiple WorkID assignments.
        """

        error_msg = f"Warning: product {isbn} has multiple work ID assignments: {manifestations}"
        self.errors.append(error_msg)

    def get_works_lookup_table(self) -> List[dict]:
        """
        Aggregate the products into works, and output a list of dicts ready for jsonline conversion and BQ loading.
        Keys: ISBN, Work ID.

        :return: List of dicts.
        """

        lookup_entries = list()

        for work in self.works:
            for isbn in work.isbns:
                mapping = {"isbn13": str(isbn), "work_id": str(work.work_id)}
                lookup_entries.append(mapping)

        return lookup_entries


class BookWorkFamilyAggregator:
    """
    Aggregates different editions of works into a family.  The methodology will be similar to BookWorkAggregator.
    This works with lists of BookWork objects rather than product records, so you need to have already aggregated
    products to works.
    """

    def __init__(self, works: List[BookWork]):
        """
        :param works: List of work objects.
        """

        self.works = works
        self.relevant_product_codes = self.set_relevant_product_codes()
        self.work_families = list()

    def set_relevant_product_codes(self) -> Set[str]:
        """
        :return: Set of relevant product codes indicating different editions.
        """

        return set(
            [
                "Replaces",
                "Replaced by",
                "Is special edition of",
                "Has special edition",
                "Is prebound edition of",
                "Is original of prebound edition",
                "Is facsimile of",
                "Is original of facsimile",
                "Is later edition of first edition",
            ]
        )

    def aggregate(self) -> List[BookWorkFamily]:
        """
        Run the aggregation process.
        Things that hint at edition information:
          1. "Replaces", "Replaced by", "Is later edition of first edition" relation in RelatedProducts.
          2. [Not implemented] "Derived from" and "Related work is derived from this" relation in RelatedWorks.
             This might need to be supplemented with other info.
          3. [Not implemented] Title, Authors, EditionNumber.
        :return: List of book work families.
        """

        self.work_families = self.agg_relproducts()
        return self.work_families

    def get_identifier_to_index_table(self, identifier: str) -> Dict:
        """
        Create a lookup table mapping identifiers to the index of the works list.
        :param identifier: Identifier name, e.g., ISBN13.
        :return: Lookup table.
        """

        n = len(self.works)
        id_to_index = {}

        for i in range(n):
            work = self.works[i]

            for product in work.products:
                if identifier in product:
                    pid = product[identifier]
                    id_to_index[pid] = i

        return id_to_index

    def get_wid_idx(
        self, pid_type: str, pid: str, isbn13_to_index: dict, gtin13_to_index: dict, proprietary_to_index: dict
    ) -> Union[None, int]:
        """
        Get the index into the works list for the product id.

        :param pid_type: Product identifier type.
        :param pid: Product ID.
        :param isbn13_to_index: ISBN lookup table.
        :param gtin13_to_index: GTIN13 lookup table.
        :param proprietary_to_index: Proprietary ID lookup table.
        :return: Index for the work in the works list, or None if the record doesn't exist.
        """

        if pid_type == "ISBN13":
            if pid not in isbn13_to_index:
                return None
            return isbn13_to_index[pid]
        elif pid_type == "GTIN_13":
            if pid not in gtin13_to_index:
                return None
            return gtin13_to_index[pid]
        elif pid_type == "PID_Proprietary":
            if pid not in proprietary_to_index:
                return None
            return proprietary_to_index[pid]
        else:
            raise Exception(f"No handling implemented for {pid_type}")

    def is_relevant_product_relation(self, relprod: dict) -> bool:
        """
        Check whether a related product contains a code indicating different (equivalent content) editions.
        :param relprod: Related product.
        :return: Whether the related product is a different edition.
        """

        codes = set(relprod["ProductRelationCodes"])

        for relcode in self.relevant_product_codes:
            if relcode in codes:
                return True

        return False

    def link_related_products(self) -> List[List[int]]:
        """
        Partition the works into equivalence classes of work families based on product relation codes.
        :return: Partition of the works into work families (using work indices of the works list).
        """

        n = len(self.works)
        gtin13_to_index = self.get_identifier_to_index_table("GTIN_13")
        isbn13_to_index = self.get_identifier_to_index_table("ISBN13")
        proprietary_to_index = self.get_identifier_to_index_table("PID_Proprietary")

        uf = UnionFind(n)
        for work_idx in range(n):
            work = self.works[work_idx]
            for product in work.products:
                for relprod in product["RelatedProducts"]:
                    if not self.is_relevant_product_relation(relprod):
                        continue

                    pid_type, pid = get_pref_product_id(relprod)
                    wid = self.get_wid_idx(pid_type, pid, isbn13_to_index, gtin13_to_index, proprietary_to_index)

                    # BookWorkAggregator should be logging all errors. No need for extra logging here.
                    if wid is None:
                        continue

                    uf.unite(work_idx, wid)

        return uf.get_partition()

    def agg_relproducts(self) -> List[BookWorkFamily]:
        """
        Collect the entries with "Replaces", "Replaced by", "Is later edition of first edition" relation codes into a single family.  The Work Family ID will be an
        arbitrary WorkID representative.
        :return: List of BookWork objects that categorise the product records.
        """

        partition = self.link_related_products()
        workfamilies = list()

        for part in partition:
            works = [self.works[i] for i in part]
            work_family_id = works[0].work_id  # Take a representative
            work_family_id_type = works[0].work_id_type
            workfamilies.append(
                BookWorkFamily(works=works, work_family_id=work_family_id, work_family_id_type=work_family_id_type)
            )

        return workfamilies

    def get_works_family_lookup_table(self) -> Dict:
        """
        Aggregate the works into work families, and output a list of dicts ready for jsonline conversion and BQ loading.
        Keys: ISBN, Work family ID.

        :return: List of dicts.
        """

        lookup_entries = list()

        for work_family in self.work_families:
            for work in work_family.works:
                for isbn in work.isbns:
                    mapping = {"isbn13": str(isbn), "work_family_id": str(work_family.work_family_id)}
                    lookup_entries.append(mapping)

        return lookup_entries
