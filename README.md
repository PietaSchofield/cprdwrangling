DuckDB and R</br>for (CPRD)</br>Data Analysis
================
Dr Pietà-Georgina ‘Georgie’ Schofield
2026-01-21

- [Motivation](#motivation)
  - [CPRD Aurum Data](#cprd-aurum-data)
- [Problem](#problem)
  - [Find Index Age Distribution by Dementia
    Type](#find-index-age-distribution-by-dementia-type)
- [A Solution](#a-solution)
  - [`duckdb`, `tidyverse`, `DBI` and
    `dbplyr`](#duckdb-tidyverse-dbi-and-dbplyr)
- [Example ETL](#example-etl)
  - [Extract](#extract)
  - [Load](#load)
  - [Have a cup of coffee (go for
    lunch)](#have-a-cup-of-coffee-go-for-lunch)
  - [Add the Code-list](#add-the-code-list)
  - [The Database](#the-database)
- [What Happened to T(ransform)](#what-happened-to-transform)
  - [Data Quality Issues](#data-quality-issues)
- [Example](#example)
  - [`dbplyr`](#dbplyr)
  - [`tibble`](#tibble)
  - [`ggplot2`](#ggplot2)
  - [Uh oh!](#uh-oh)
- [This is “Lazy Evaluation” </br></br>chain `dbplyr` then
  collect()](#this-is-lazy-evaluation-chain-dbplyr-then-collect)
  - [Step 1.](#step-1)
  - [Step 1.](#step-1-1)
  - [Warning](#warning)
  - [Step 2.](#step-2)
  - [Step 3.](#step-3)
  - [Step 3.](#step-3-1)
  - [Replot](#replot)
- [Discussion](#discussion)
  - [What This Talk Was Not](#what-this-talk-was-not)
  - [What This Talk Was](#what-this-talk-was)
  - [Take Home Message](#take-home-message)
- [Thank You
  </br>pietas@liverpool.ac.uk</br>https://github.com/PietaSchofield](#thank-you-pietasliverpoolacukhttpsgithubcompietaschofield)

# Motivation

## CPRD Aurum Data

- 250000 dementia patient records from CPRD Aurum
- 13GB of data in 9 compressed files (each ~2GB)
- Unzips to 82GB of data
  - patient: 249961 records in 1 file
  - practice: 1483 records in 1 file
  - staff: 1336287 records in 1 file
  - consultation: 120456179 records in 10 files
  - observation: 425006899 records in 43 file
  - referral: 2826888 records in 1 file
  - problem: 14501709 records in 1 file(s)
  - drugissue: 197222175 records in 35

# Problem

## Find Index Age Distribution by Dementia Type

- You have a laptop and R and limited memory
- You need to find index date for a number of codes groups representing
  different dementia types
- R freezes or just crashes if you try to load 425 Million Records from
  43 files
- You could load each in turn and filter save but…
  - What about patients spanning more than one file
  - At some point you need to realise there are data quality issues to
    sort out
  - Repeat the whole process

# A Solution

## `duckdb`, `tidyverse`, `DBI` and `dbplyr`

- `tidyverse`: (`tibble`, `dplyr`, `ggplot2`, `lubridate` etc.)
  - Other alternative object paradigms exist data.tables but I am at
    home in the `tidyverse`
- duckdb: is a column orientated local file based `sql` database engine
  - specially tuned for analytics (OLAP) over transaction processing
    (OLTP)
  - similar but much better data science alternative to SQLite
- `DBI`: connect R to databases
  - `dbplyr` permits use of `tidyverse` functions of database tables
    rather than `tibbles`
  - can use SQL queries that is your preference.

# Example ETL

## Extract

- “CPRD fob-holder” has done that and you now have your 9 zipped files
  (13GB)
- Copy them from CPRDGeneralinfo drive to a University asset you have
  read/write access to
  - options university desktop / laptop / active data storage network
    drive (preferable)
  - **do not copy to unencrypted non_password protected devices (eg.
    penstick, personal laptops)**
- Unzip the compressed files into the 93 tab delimited text files (82GB)

## Load

- Install the `rtrhd` R package (GitHub)

``` r
install.packages("remotes")
remotes::install_github("PietaSchofield/rtrhd")
```

- Get a copy of the CPRD Aurum/EMIS data-dictionary file from
  CPRDGeneralinfo drive
- Then run load_with_data_dictionary (works for EMIS, GOLD, HES, ONS…)

``` r
rtrhd::load_with_data_dictionary(
  ddFile=file.path(...), # location of data dictionary file
  dbf=file.path(...), # location of duckdb database file to be created
  datadir=file.path(...), # location the text files that were unzipped 
  dset="..." # a prefix for tablenames eg. 'emis' or 'aurum' or 'gold'
)
```

## Have a cup of coffee (go for lunch)

<div style="font-size: 0.8em;">

\></br> INFO \[2026-01-16 16:54:35\] emis_patient: 249961 records loaded
from 1 file(s) </br> INFO \[2026-01-16 16:54:36\] emis_practice: 1483
records loaded from 1 file(s) </br> INFO \[2026-01-16 16:54:37\]
emis_staff: 1336287 records loaded from 1 file(s) </br> INFO
\[2026-01-16 16:58:22\] emis_consultation: 120456179 records loaded from
10 file(s) </br> INFO \[2026-01-16 17:17:25\] emis_observation:
425006899 records loaded from 43 file(s) </br> INFO \[2026-01-16
17:17:29\] emis_referral: 2826888 records loaded from 1 file(s) </br>
INFO \[2026-01-16 17:17:52\] emis_problem: 14501709 records loaded from
1 file(s) </br> INFO \[2026-01-16 17:27:34\] emis_drugissue: 197222175
records loaded from 35 file(s) </br> \>

</div>

- 18GB duckdb file created (you can delete the 83GB of text files now)
- 761,601,581 records ingested in 1978 seconds (~33 minutes)
- Still think you need a database server?

## Add the Code-list

``` r
coi <- file.path(.projLoc,"dementia_codes.xlsx") |>
  readxl::read_excel(col_type="text") |>
  mutate(
    preferred_term=iconv(preferred_term, 
                         from="UTF-8", to="ASCII//TRANSLIT"),
    proposed_group=iconv(proposed_group, 
                         from="UTF-8", to="ASCII//TRANSLIT")
 ) 

rtrhd::load_table(dbf=dbname,dataset=coi,tab_name="coi_emis_dementia")

rtrhd::list_db(dbname)
```

## The Database

<div style="font-size: 0.5em;">

</div>

| tablename | fields | records |
|:---|:---|---:|
| coi_emis_dementia | medcodeid, snomedctconceptid, preferred_term, proposed_group, n_synonyms | 671 |
| emis_consultation | patid, consid, pracid, consdate, enterdate, staffid, conssourceid, cprdconstype, consmedcodeid | 120456179 |
| emis_drugissue | patid, issueid, pracid, probobsid, drugrecid, issuedate, enterdate, staffid, prodcodeid, dosageid, quantity, quantunitid, duration, estnhscost | 197222175 |
| emis_observation | patid, consid, pracid, obsid, obsdate, enterdate, staffid, parentobsid, medcodeid, value, numunitid, obstypeid, numrangelow, numrangehigh, probobsid | 425006899 |
| emis_patient | patid, pracid, usualgpstaffid, gender, yob, mob, emis_ddate, regstartdate, patienttypeid, regenddate, acceptable, cprd_ddate | 249961 |
| emis_practice | pracid, lcd, uts, region | 1483 |
| emis_problem | patid, obsid, pracid, parentprobobsid, probenddate, expduration, lastrevdate, lastrevstaffid, parentprobrelid, probstatusid, signid | 14501709 |
| emis_referral | patid, obsid, pracid, refsourceorgid, reftargetorgid, refurgencyid, refservicetypeid, refmodeid | 2826888 |
| emis_staff | staffid, pracid, jobcatid | 1336287 |

# What Happened to T(ransform)

## Data Quality Issues

- There will be lots of data quality issues you need to understand.
- It is routinely collected healthcare data afterall
- Data quality filters such practice
  - Up To Standard Date (uts) (but this is not currently supplied, false
    confidence?)
  - Last Collected Date (lcd)
- These are much better understood if
  - You can easily explore your data
  - You can run many look-see queries
  - You can visualise the results

# Example

## `dbplyr`

``` r
# connect to the database
dbcon <- DBI::dbConnect(duckdb::duckdb(),dbname,read_only=T)
# build the query
dementia_trends <- tbl(dbcon,"emis_observation") |>
  inner_join(tbl(dbcon,"coi_emis_dementia"), by = "medcodeid") |>
  group_by(patid,proposed_group) |> window_order(obsdate) |>
  summarise(
    index_date=first(obsdate),
    .groups="drop") |>
  inner_join(tbl(dbcon,"emis_patient") |>
             transmute(patid,yob,gender,regstartdate), by = "patid") |>
  mutate(index_year=year(index_date),index_age=index_year-yob) |>
# pull the data to R
  collect()
# disconnect the database
DBI::dbDisconnect(dbcon)
```

## `tibble`

<div style="font-size: 0.5em;">

</div>

| patid            | proposed_group             |  yob | gender | index_year | index_age |
|:-----------------|:---------------------------|-----:|-------:|-----------:|----------:|
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1956 |      2 |       2024 |        68 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1966 |      2 |       2018 |        52 |
| \*\*\*\*\*\*\*\* | Vascular                   | 1961 |      2 |       2025 |        64 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1949 |      2 |       2024 |        75 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1948 |      1 |       2017 |        69 |
| \*\*\*\*\*\*\*\* | Other/Check                | 1947 |      1 |       2023 |        76 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1956 |      1 |       2018 |        62 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1936 |      2 |       2018 |        82 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1946 |      2 |       2020 |        74 |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | 1942 |      2 |       2014 |        72 |

## `ggplot2`

``` r
dementia_trends_plot <- dementia_trends %>%
  ggplot(aes(x = index_age, fill = proposed_group)) +
  geom_boxplot(alpha = 0.7) +
  labs(
    title = "Age at First Dementia Diagnosis by Type (250K patients 425M Observations)",
    x = "Age at Index Diagnosis",
    y = "Density"
  ) +
  theme_minimal()
```

## Uh oh!

<figure>
<img
src="https://github.com/PietaSchofield/cprdwrangling/tree/main/README_files/figure-gfm/plotit_R-1.png"
alt="Dementia trends plot" />
<figcaption aria-hidden="true">Dementia trends plot</figcaption>
</figure>

# This is “Lazy Evaluation” </br></br>chain `dbplyr` then collect()

## Step 1.

``` r
dbcon <- DBI::dbConnect(duckdb::duckdb(), dbname, read_only=T)
dementia_trends <- tbl(dbcon, "emis_observation") |>
  inner_join(tbl(dbcon, "coi_emis_dementia"), by = "medcodeid") |>
  inner_join(tbl(dbcon, "emis_patient"), by = c("patid","pracid")) |>
  transmute(patid,pracid,yob,sex=gender,regstart=regstartdate,regend=regenddate,
            obsid,obsdate,enterdate,medcodeid,dementia_type=proposed_group)
```

## Step 1.

<div style="font-size: 0.8em; white-space: pre; overflow-x: auto;">

\> is(dementia_trends)  
\[1\] “tbl_duckdb_connection”  
\> dementia_trends \|\>
mutate(patid=“\*\*\*\*”,regstart=“\*\*\*\*”,regend=“\*\*\*\*\*”) \|\>
head()  
\# Source: SQL \[?? x 11\]  
\# Database: DuckDB 1.4.3 <user@Linux> 6.18.4-1-MANJARO:R
4.5.2//home/user/Projects/demrfmr/.data/duckdb/demrisk.duckdb  
patid pracid yob sex regstart regend obsid obsdate enterdate medcodeid
dementia_type  
\<chr\> \<int\> \<int\> \<int\> \<chr\> \<chr\> \<chr\> \<date\>
\<date\> \<chr\> \<chr\>  
1 \*\*\*\* 20002 1970 1 \*\*\*\* \*\*\*\*\* 17757124480 2022-11-03
2022-11-03 1867041000006112 Unspecified/Other dementia  
2 \*\*\*\* 20002 1958 1 \*\*\*\* \*\*\*\*\* 1158071968 2014-11-27
2014-11-27 1867041000006112 Unspecified/Other dementia  
3 \*\*\*\* 20002 1961 1 \*\*\*\* \*\*\*\*\* 23115694179 2024-06-17
2024-06-17 1867041000006112 Unspecified/Other dementia  
4 \*\*\*\* 20002 1940 2 \*\*\*\* \*\*\*\*\* 10295072833 2020-03-10
2020-03-10 1150521000000117 Unspecified/Other dementia  
5 \*\*\*\* 20002 1929 2 \*\*\*\* \*\*\*\*\* 7348443459 2019-06-07
2019-06-18 45046017 Alzheimera??s  
6 \*\*\*\* 20002 1920 2 \*\*\*\* \*\*\*\*\* 1161784698 2017-01-13
2017-01-13 2345931000000111 Unspecified/Other dementia  
\> dementia_trends \|\> tally()  
\# Source: SQL \[?? x 1\]  
\# Database: DuckDB 1.4.3 <user@Linux> 6.18.4-1-MANJARO:R
4.5.2//home/user/Projects/demrfmr/.data/duckdb/demrisk.duckdb  
n  
\<dbl\>  
1 787996

</div>

## Warning

- Many R display/formatting functions will **silently trigger
  `collect()`** on lazy queries:
- For example will all trigger collect() and potentially crash R:
  - kable(huge_lazy_query) \# knitr::kable()
  - DT::datatable(huge_lazy_query) \# DT package
  - print(huge_lazy_query) \# sometimes, depending on print method
  - View(huge_lazy_query) \# RStudio viewer

## Step 2.

``` r
dementia_trends <- dementia_trends |>
  inner_join(tbl(dbcon, "emis_practice") |> transmute(pracid, lcd), 
    by= "pracid") |>
  filter(
    obsdate >= regstart,
    obsdate <= coalesce(lcd,coalesce(regend, as.Date('2024-12-31')))
  ) |> 
  group_by(patid, sex, yob, dementia_type) |>
  window_order(obsdate) |>
  summarise(
    index_date = first(obsdate),
    .groups = "drop"
  ) 
```

## Step 3.

``` r
dementia_trends <- dementia_trends |>
  mutate(
    index_year = year(index_date),
    index_age = index_year - yob
  ) |>
  # Additional quality filters in DuckDB
  filter(
    index_age >= 0,
    index_age <= 110
  ) |>
  collect() |>
  mutate( dementia_type = gsub("a\\?\\?","'",dementia_type))
DBI::dbDisconnect(dbcon)
```

## Step 3.

## Replot

<figure>
<img
src="https://github.com/PietaSchofield/cprdwrangling/tree/main/README_files/figure-gfm/plotit_2_R-1.png"
alt="Dementia trends plot" />
<figcaption aria-hidden="true">Dementia trends plot</figcaption>
</figure>

# Discussion

## What This Talk Was Not

- This wasn’t a talk about how to clean your data e.g. dodgy dates
- **Answer: DEPENDS ON YOUR RESEARCH QUESTION**
  - Prevalence study? → Maybe keep patient, drop bad record
  - Disease progression study? → Whole patient sequence compromised
  - Healthcare utilization? → Registration period matters more
- An exhaustive tutorial on R, `tidyverse`, `dbplyr` or `duckdb`
- Dissing `data.tables` or `python` or `sql` any other tool/language

## What This Talk Was

- To show the potential of `duckdb`, `tidyverse` and `dbplyr`
  - for efficient data exploration
  - for effective data visualisation
- “This talk was brought to you by…”

``` r
# CRAN packages
install.packages(c("duckdb","tidyverse","dbplyr","remotes","rmarkdown"))

# My R Tools for Routine Healthcare Data (rtrhd) package
remotes::install_github("PietaSchofield/rtrhd")
```

- **WARNING on Manjaro Linux duckdb takes AGES!! to install** … no
  **really** go for dinner, watch a movie contemplate, life choices
  long,… but on Windows and Mac I understand it is almost instant

## Take Home Message

- I loaded the data it took 33 minutes
- To render these slides using `rmarkdown::render`
  - live from the database
  - counting rows in each of 9 tables
  - queries processing 425Million records twice
  - drawing two ggplots
  - **\< 15 seconds**

# Thank You </br><pietas@liverpool.ac.uk></br><https://github.com/PietaSchofield>
