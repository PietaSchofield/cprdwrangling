DuckDB and R for Data Analysis
================
Dr Pietà-Georgina ‘Georgie’ Schofield
2026-01-20

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
- [This is “Lazy Evaluation” </br></br>Spoiler Alert: chain `dbplyr`
  then
  collect()](#this-is-lazy-evaluation-spoiler-alert-chain-dbplyr-then-collect)
  - [Step 1.](#step-1)
  - [Step 2.](#step-2)
  - [Step 3.](#step-3)
  - [Replot](#replot)
- [Discussion](#discussion)
  - [What This Talk Was Not](#what-this-talk-was-not)
  - [What This Talk Was](#what-this-talk-was)
  - [Take Home Message](#take-home-message)
- [Thank You (pietas@liverpool.ac.uk)](#thank-you-pietasliverpoolacuk)

    Loading required package: tidyverse

    ── Attaching core tidyverse packages ────────────────────────────────────────────────────────────────────────────────────────────── tidyverse 2.0.0 ──
    ✔ dplyr     1.1.4     ✔ readr     2.1.6
    ✔ forcats   1.0.0     ✔ stringr   1.6.0
    ✔ ggplot2   4.0.1     ✔ tibble    3.3.1
    ✔ lubridate 1.9.4     ✔ tidyr     1.3.1
    ✔ purrr     1.2.1     
    ── Conflicts ──────────────────────────────────────────────────────────────────────────────────────────────────────────────── tidyverse_conflicts() ──
    ✖ dplyr::filter() masks stats::filter()
    ✖ dplyr::lag()    masks stats::lag()
    ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
    Loading required package: dbplyr


    Attaching package: 'dbplyr'


    The following objects are masked from 'package:dplyr':

        ident, sql

# Motivation

## CPRD Aurum Data

- 250000 dementia patient records from CPRD Aurum
- 13GB of data in 7 compressed files (each ~2GB)
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

- Well I have done that and you have your 9 zipped files (13GB)
- Copy them from CPRDGeneralinfo drive to a University asset you have
  read/write access to
  - options university desktop / laptop / active data storage network
    drive (preferable)
- Unzip the compressed files into the 93 tab delimited text files (82GB)

## Load

- Download my `rtrhd` R package (GitHub) (NB. It has a lot of irrelevant
  functions)
- get a copy of the CPRD Aurum data-dictionary file from CPRDGeneralinfo
  drive
- Then run this

<div style="font-size: 0.8em;">

``` r
# You will need the location of your files here so I have just put variables
rtrhd::load_with_data_dictionary(
  ddFile=ddfile, # Variable holding location of data-
                 # dictionary file
  dbf=dbname, # Variable holding location of duckdb database file 
              # to be created
  datadir=ddir, # Variable pointing to parent directory where the
                # text files were unzipped to
  dset=dset # Variable prefix for database tables to be created
            # eg. emis or aurum or gold
)
```

</div>

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

- 18GB duckdb file created
- 761,601,581 records ingested in 1978 seconds (~33 minutes)
- Still think you need a database server?

## Add the Code-list

<div style="font-size: 0.8em;">

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
```

</div>

## The Database

<div style="font-size: 0.5em;">

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

</div>

# What Happened to T(ransform)

## Data Quality Issues

- There will be lots of data quality issues you need to understand. (It
  is routinely collected healthcare data!)
- These are much better understood if
  - You can easily explore your data
  - You can run many look-see queries
  - You can visualise the results

# Example

## `dbplyr`

<div style="font-size: 0.8em;">

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

</div>

## `tibble`

<div style="font-size: 0.5em;">

</div>

| patid | proposed_group | index_date | yob | gender | regstartdate | index_year | index_age | regstartdat |
|:---|:---|:---|---:|---:|:---|---:|---:|:---|
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1951 | 1 | 1997-02-21 | 2016 | 65 | —- |
| \*\*\*\*\*\*\*\* | Alzheimera??s | —- | 1937 | 2 | 1986-04-14 | 2016 | 79 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1936 | 1 | 2000-05-09 | 2019 | 83 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1966 | 2 | 1997-07-11 | 2019 | 53 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1977 | 2 | 2000-05-09 | 2017 | 40 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1949 | 1 | 1997-02-14 | 2017 | 68 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1956 | 1 | 1986-04-14 | 2020 | 64 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1968 | 2 | 1986-04-14 | 2015 | 47 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1962 | 2 | 1992-04-14 | 2019 | 57 | —- |
| \*\*\*\*\*\*\*\* | Unspecified/Other dementia | —- | 1951 | 1 | 2000-05-19 | 2022 | 71 | —- |

## `ggplot2`

<div style="font-size: 0.8em;">

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

</div>

## Uh oh!

<figure>
<img
src="https://github.com/PietaSchofield/cprdwrangling/tree/main/README_files/figure-gfm/plotit_R-1.png"
alt="Dementia trends plot" />
<figcaption aria-hidden="true">Dementia trends plot</figcaption>
</figure>

# This is “Lazy Evaluation” </br></br>Spoiler Alert: chain `dbplyr` then collect()

## Step 1.

<div style="font-size: 0.8em;">

``` r
dbcon <- DBI::dbConnect(duckdb::duckdb(), dbname, read_only=T)

dementia_trends <- tbl(dbcon, "emis_observation") |>
  inner_join(tbl(dbcon, "emis_patient") |>
    select(patid, yob, gender, regstartdate, regenddate), 
    by = "patid") |>
  # Filter early in DuckDB - much more efficient
  filter(
    obsdate >= regstartdate,
    # Use today's date or end of data
    obsdate <= coalesce(regenddate, as.Date('2024-12-31'))
  ) 
```

</div>

## Step 2.

<div style="font-size: 0.8em;">

``` r
dementia_trends <- dementia_trends |>
  inner_join(tbl(dbcon, "coi_emis_dementia"), by = "medcodeid") %>%
  group_by(patid, proposed_group) %>% 
  window_order(obsdate) %>%
  summarise(
    index_date = first(obsdate),
    .groups = "drop"
  ) %>%
  inner_join(tbl(dbcon, "emis_patient") %>% 
    transmute(patid, yob, gender, regstartdate), 
    by = "patid") 
```

</div>

## Step 3.

<div style="font-size: 0.8em;">

``` r
dementia_trends <- dementia_trends |>
  mutate(
    index_year = year(index_date),
    index_age = index_year - yob
  ) %>%
  # Additional quality filters in DuckDB
  filter(
    index_age >= 18,
    index_age <= 110
  ) %>%
  collect()
DBI::dbDisconnect(dbcon)
```

</div>

## Replot

<div style="font-size: 0.8em;">

</div>

<figure>
<img
src="https://github.com/PietaSchofield/cprdwrangling/tree/main/README_files/figure-gfm/plotit_2_R-1.png"
alt="Dementia trends plot" />
<figcaption aria-hidden="true">Dementia trends plot</figcaption>
</figure>

# Discussion

## What This Talk Was Not

- This wasn’t a talk about how to clean your data
- An exhaustive tutorial on R, `tidyverse`, `dbplyr` or `duckdb`
- Dissing `data.tables` or `python` or `sql` any other tool/language

## What This Talk Was

- To show the potential of `duckdb` and `dbplyr`
- “This talk was brought to you by…”
  <div style="font-size: 0.8em;">

  </div>

``` r
# CRAN packages
install.packages(c("duckdb","tidyverse","dbplyr","remotes","rmarkdown"))

# My R Tools for Routine Healthcare Data (rtrhd) package 
# (and its dependencies)
remotes::install_github("PietaSchofield/rtrhd")
```

</div>

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

# Thank You (<pietas@liverpool.ac.uk>)
