# Fundref

“The Funder Registry and associated funding metadata allows everyone to have transparency 
into research funding and its outcomes. It’s an open and unique registry of persistent 
identifiers for grant-giving organizations around the world.”
 _- source: [Funder Registry](https://www.crossref.org/services/funder-registry/)_ 
and [data details](https://github.com/CrossRef/rest-api-doc)

---

**Schema**

+ **parents** [*Record*]
    + **parent** [*Record*]
        + **parent** [*Record*]
            + **parent** [*Record*]
                + **parent** [*String*]
                + **name** [*String*]
                + **funder** [*String*]
            + **name** [*String*]
            + **funder** [*String*]
        + **name** [*String*]
        + **funder** [*String*]
    + **name** [*String*]
    + **funder** [*String*]
+ **bottom** [*Boolean*]
+ **notation** [*String*]
+ **formly_known_as** [*String*]
+ **split_from** [*String*]
+ **merger_of** [*String*]
+ **status** [*String*]
+ **children** [*Record*]
    + **children** [*Record*]
        + **children** [*Record*]
            + **children** [*Record*]
                + **children** [*String*]
                + **name** [*String*]
                + **funder** [*String*]
            + **name** [*String*]
            + **funder** [*String*]
        + **name** [*String*]
        + **funder** [*String*]
    + **name** [*String*]
    + **funder** [*String*]
+ **split_into** [*String*]
+ **region** [*String*]
+ **is_replaced_by** [*String*]
+ **funder** [*String*]
+ **replaces** [*String*]
+ **incorporated_into** [*String*]
+ **affil_with** [*String*]
+ **renamed_as** [*String*]
+ **alt_label** [*String*]
+ **merged_with** [*String*]
+ **tax_id** [*String*]
+ **state** [*String*]
+ **country** [*String*]
+ **incorporates** [*String*]
+ **continuation_of** [*String*]
+ **country_code** [*String*]
+ **funding_body_sub_type** [*String*]
+ **pre_label** [*String*]
+ **funding_body_type** [*String*]
+ **created** [*Timestamp*]
+ **broader** [*String*]
+ **top** [*Boolean*]
+ **narrower** [*String*]
+ **modified** [*Timestamp*]

