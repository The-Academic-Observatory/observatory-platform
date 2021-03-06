# Global Research Identifier Database

The Global Research Integer Database (GRID) includes data on institutes including acronyms, 
addresses, aliases, external identifiers, geographical name data, labels, links, 
relationships, types, and whether it is an active institution
. _- source: [GRID](https://www.grid.ac/)_ 
and [data details](https://www.grid.ac/format)

---

**Schema for data extraction prior to 2015-09-22**

+ **redirect** [*String*]
+ **relationships** [*Record*]
    + **id** [*String*]
    + **label** [*String*]
    + **type** [*String*]
+ **external_ids** [*Record*]
    + **FundRef** [*String*]
    + **ISNI** [*String*]
    + **OrgRef** [*String*]
    + **Wikidata** [*String*]
    + **HESA** [*String*]
    + **UCAS** [*String*]
    + **UKPRN** [*String*]
    + **CNRS** [*String*]
    + **LinkedIn** [*String*]
+ **status** [*String*]
+ **id** [*String*]
+ **wikipedia_url** [*String*]
+ **email_address** [*String*]
+ **links** [*String*]
+ **established** [*Integer*]
+ **ip_addresses** [*String*]
+ **name** [*String*]
+ **aliases** [*String*]
+ **weight** [*Integer*]
+ **acronyms** [*String*]
+ **types** [*String*]
+ **addresses** [*Record*]
    + **geonames_city** [*Record*]
        + **license** [*Record*]
            + **license** [*String*]
            + **attribution** [*String*]
        + **geonames_admin2** [*Record*]
            + **code** [*String*]
            + **ascii_name** [*String*]
            + **name** [*String*]
        + **nuts_level3** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **nuts_level2** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **geonames_admin1** [*Record*]
            + **code** [*String*]
            + **ascii_name** [*String*]
            + **name** [*String*]
        + **nuts_level1** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **city** [*String*]
        + **id** [*Integer*]
    + **country_code** [*String*]
    + **line_1** [*String*]
    + **country** [*String*]
    + **line_3** [*String*]
    + **city** [*String*]
    + **line_2** [*String*]
    + **lng** [*Float*]
    + **postcode** [*String*]
    + **primary** [*Boolean*]
    + **lat** [*Float*]
    + **state** [*String*]
    + **state_code** [*String*]
+ **labels** [*Record*]
    + **iso639** [*String*]
    + **label** [*String*]

---

**Schema for data extraction from 2016-04-28 onwards**

+ **redirect** [*String*]
+ **relationships** [*Record*]
    + **id** [*String*]
    + **label** [*String*]
    + **type** [*String*]
+ **external_ids** [*Record*]
    + **UKPRN** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **UCAS** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **OrgRef** [*Record*]
        + **all** [*String*]
        + **preferred** [*Integer*]
    + **FundRef** [*Record*]
        + **all** [*String*]
        + **preferred** [*Integer*]
    + **HESA** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **Wikidata** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **ISNI** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **ROR** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **CNRS** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
    + **LinkedIn** [*Record*]
        + **all** [*String*]
        + **preferred** [*String*]
+ **status** [*String*]
+ **id** [*String*]
+ **wikipedia_url** [*String*]
+ **email_address** [*String*]
+ **links** [*String*]
+ **established** [*Integer*]
+ **ip_addresses** [*String*]
+ **name** [*String*]
+ **aliases** [*String*]
+ **weight** [*Integer*]
+ **acronyms** [*String*]
+ **types** [*String*]
+ **addresses** [*Record*]
    + **geonames_city** [*Record*]
        + **license** [*Record*]
            + **license** [*String*]
            + **attribution** [*String*]
        + **geonames_admin2** [*Record*]
            + **code** [*String*]
            + **ascii_name** [*String*]
            + **name** [*String*]
        + **nuts_level3** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **nuts_level2** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **geonames_admin1** [*Record*]
            + **code** [*String*]
            + **ascii_name** [*String*]
            + **name** [*String*]
        + **nuts_level1** [*Record*]
            + **code** [*String*]
            + **name** [*String*]
            + **ascii_name** [*String*]
        + **city** [*String*]
        + **id** [*Integer*]
    + **country_code** [*String*]
    + **line_1** [*String*]
    + **country** [*String*]
    + **line_3** [*String*]
    + **city** [*String*]
    + **line_2** [*String*]
    + **lng** [*Float*]
    + **postcode** [*String*]
    + **primary** [*Boolean*]
    + **lat** [*Float*]
    + **state** [*String*]
    + **state_code** [*String*]
+ **labels** [*Record*]
    + **iso639** [*String*]
    + **label** [*String*]

