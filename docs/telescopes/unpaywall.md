# Telescope: Unpaywall

- - -

**Dataset type**: Bibliographic data

- - - 
## Dataset Schema

+ **journal_is_in_doaj** [*Boolean*] Nullable
+ **published_date** [*Date*] Nullable
+ **journal_issns** [*String*] Nullable
+ **journal_issn_l** [*String*] Nullable
+ **issn_l** [*String*] Nullable
+ **data_standard** [*Integer*] Nullable
+ **x_reported_noncompliant_copies** [*Record*] Repeated
    + **blank** [*String*] Nullable
+ **oa_locations** [*Record*] Repeated
    + **pmh_id** [*String*] Nullable
    + **url** [*String*] Nullable
    + **license** [*String*] Nullable
    + **url_for_landing_page** [*String*] Nullable
    + **version** [*String*] Nullable
    + **url_for_pdf** [*String*] Nullable
    + **is_best** [*Boolean*] Nullable
    + **updated** [*Timestamp*] Nullable
    + **evidence** [*String*] Nullable
    + **host_type** [*String*] Nullable
    + **repository_institution** [*String*] Nullable
    + **endpoint_id** [*String*] Nullable
    + **id** [*String*] Nullable
+ **doi** [*String*] Nullable
+ **best_oa_location** [*Record*] Nullable
    + **pmh_id** [*String*] Nullable
    + **url** [*String*] Nullable
    + **license** [*String*] Nullable
    + **url_for_landing_page** [*String*] Nullable
    + **version** [*String*] Nullable
    + **url_for_pdf** [*String*] Nullable
    + **is_best** [*Boolean*] Nullable
    + **updated** [*Timestamp*] Nullable
    + **evidence** [*String*] Nullable
    + **host_type** [*String*] Nullable
    + **repository_institution** [*String*] Nullable
    + **endpoint_id** [*String*] Nullable
    + **id** [*String*] Nullable
+ **updated** [*Timestamp*] Nullable
+ **oa_status** [*String*] Nullable
+ **title** [*String*] Nullable
+ **genre** [*String*] Nullable
+ **year** [*Integer*] Nullable
+ **is_oa** [*Boolean*] Nullable
+ **publisher** [*String*] Nullable
+ **doi_url** [*String*] Nullable
+ **z_authors** [*Record*] Repeated
    + **sequence** [*String*] Nullable
    + **authenticated_orcid** [*Boolean*] Nullable
    + **suffix** [*String*] Nullable
    + **affiliation** [*Record*] Repeated
        + **name** [*String*] Nullable
    + **name** [*String*] Nullable
    + **ORCID** [*String*] Nullable
    + **family** [*String*] Nullable
    + **given** [*String*] Nullable
+ **journal_is_oa** [*Boolean*] Nullable
+ **has_repository_copy** [*Boolean*] Nullable
+ **x_error** [*Boolean*] Nullable
+ **journal_name** [*String*] Nullable
- - - 
