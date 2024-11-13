USE layoffs_data;

# ------- Data cleaning

# --- Import dataset. I have used the Table Data Import Wizards functionality; it's just easier and more straightforward for this small dataset.

## Check for unique categories in some variables.
SELECT DISTINCT industry
FROM layoffs;
SELECT DISTINCT stage
FROM layoffs;

# --- Create a staging table so that data can be safely manipulated without the risk of losing the raw data.
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

## From now on, I'll be working mainly with the staging table.

# --- Remove duplicates.
/*
First, identify the duplicate rows.
Use the window function over(partition by) to create a partition of rows based on many different columns so that you can tell whether that *record* is unique or not (row_number).
Put this query in a CTE for improved management of the query.
*/

WITH duplicate_find AS (
	SELECT *, ROW_NUMBER() OVER (
		PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) AS record_number
	FROM layoffs_staging
)
SELECT *
FROM duplicate_find
WHERE record_number > 1;

## Now I will double-check some of the "possible" duplicates by sampling a few random ones
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

/* MySQL does not allow me to delete data from a CTE "table", which is a bummer.
Therefore, I will create a new table that contains also the column "record_number", and from there I will delete the records whose value is >1.

Alternatively, I could have created a new table with only the rows whose record number is = 1 (based on the previous CTE).
Then, I would have drop the first staging table and kept the deduplicated table. But this approach is worse in terms of data volume processing.*/

## Create the new table, without data (just headers).
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `record_number` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

## Populate the new table with all rows and "record_number" column.
INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER (
		PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) AS record_number
FROM layoffs_staging;

## Now, delete the duplicates.
DELETE
FROM layoffs_staging2
WHERE record_number > 1;

# --- Data standardization.
## Trim company names (removing unnecessary white spaces).
UPDATE layoffs_staging2
SET company = TRIM(company);

## Find redundant industry names 
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry = 'Transportation' OR industry = 'Logistics';

SELECT *
FROM layoffs_staging2
WHERE industry = 'Finance' OR industry = 'Fin-Tech';

/* Crypto Currency and CryptoCurrency and Crypto can be the same category => merged into Crypto.
Transportation and Logistics might seem the same category, but I concluded they are not (in this dataset) => unchanged.
Fin-tech clearly belongs to Finance as there are only 3 cases out of 287 rows for Finance AND Fin-tech => merged into Finance.
*/

## Standarize repetitive ones
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Finance'
WHERE industry LIKE 'Fin-Tech';

## Find redundant country names
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

/* United States has a record with a dot at the end => cleaned.
*/
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country) # Trailing, when used in conjuction with TRIM(), allows us to remove the specified character (a white space, by default) from a variable of choice
WHERE country LIKE 'United States%';

## Format and convert the date variable in a datetime type. This will be vital to work with time series methods.
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y') # running this first to check that we guessed the format right!
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); # reformatting the values in the table, but NOT the data type (next).

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; # converting the data type from string to date

# --- Dealing with Null and Blank values.
SELECT * 			# checking two key values we'll use for the analysis: if they are both NULL/empty, then we could just remove them (for later).
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ''; # There are a few cases in which known companies have no industry. That is a feasible change we could make

## Impute industry value for null/empty cells based on the value of populate cells for the same company/entity.
SELECT *
FROM layoffs_staging2 AS ls21
JOIN layoffs_staging2 AS ls22
	ON ls21.company = ls22.company
    AND ls21.location = ls22.location
    AND ls21.country = ls22.country # Joining on multiple columns (3) to ensure that we are talking about the same company.
WHERE ( ls21.industry ='' OR ls21.industry IS NULL) 
	AND ( ls22.industry IS NOT NULL OR ls22.industry != '');

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally\'s%'; # There is only one row for this, so it cannot lookup the actual industry from other records.

/* I will set all blank values from industry as Null, and then rerun the above. This should simplify the select and update statements below.*/

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT ls21.company, ls21.industry, ls22.industry
FROM layoffs_staging2 AS ls21
JOIN layoffs_staging2 AS ls22
	ON ls21.company = ls22.company
    AND ls21.location = ls22.location
    AND ls21.country = ls22.country # Joining on multiple columns (3) to ensure that we are talking about the same company.
WHERE ls21.industry IS NULL 
	AND ls22.industry IS NOT NULL;
    
UPDATE layoffs_staging2 AS ls21
JOIN layoffs_staging2 AS ls22
	ON ls21.company = ls22.company
    AND ls21.location = ls22.location
    AND ls21.country = ls22.country
SET ls21.industry = ls22.industry
WHERE ls21.industry IS NULL 
	AND ls22.industry IS NOT NULL; # Updating those values by looking up the other (populated) row with the null cell.
    

# --- Removing unnecessary columns and rows (e.g. all nulls in key values).

## Drop the "record_number" column as it served its purpose of allowing us to deduplicate the data in the previous steps.

ALTER TABLE layoffs_staging2
DROP COLUMN record_number;

## Find and decide what to do about "substantially missing" data.
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;

/* 361 rows seem to have both columns as NULL (no empty cells found).
By quickly looking at the stage, it doesn't seem that the distribution of NULL values is random.
I will check this by comparing the relative distribution of null values by stage, partitioned by whether they are BOTH NULL or else.
*/

### Observe the absolute frequency for each case (both_missing vs at_least_one_missing), comparing it to total count by stage. 

SELECT 
    stage,
    COUNT(*) as total_rows,
    SUM(CASE WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 1 ELSE 0 END) as both_missing,										# Getting the sum of the cases when both are missing
    ROUND(100.0 * SUM(CASE WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_both_missing,		# Getting the percentage frequency relative to the total number of cases (in the group)
    SUM(CASE WHEN total_laid_off IS NOT NULL OR percentage_laid_off IS NOT NULL THEN 1 ELSE 0 END) as at_least_one_present,							
    ROUND(100.0 * SUM(CASE WHEN total_laid_off IS NOT NULL OR percentage_laid_off IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_one_missing
FROM layoffs_staging2
GROUP BY stage
ORDER BY stage;

/* Looking at the absolute frequency alone doesn't tell us much.
Nothing seems to be extraordinary by a superficial look. An insight lies in the quick observation that across the Seed and Post-IPO stages, 
the rate at which both the total and percentage figures of people laid off are missing spans between 10-20%, with the peak of 21.6% for the "Acquired" stage.
Although it does not seem a statistically significant group difference, it can be explained by the fact that M&A operations are highly uncertain and 
a public commitment containing a predetermined figure could lead to under/overestimations of actual needs.

Nevertheless, it might be worthwhile to investigate whether there is a more substantial sub-pattern by looking at what exactly is not publicly announced, namely the total number or the percentage being laid off.
*/

### Break down layoff events by funding stage and pattern (both missing, only percentage, only total, both present), to then calculate total count, relative frequency of the pattern within the stage, and the relative frequency of the stage-pattern pair over the total count (i.e. 2356).

WITH pattern_counts AS (
    SELECT 
        stage,
        CASE 
            WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 'both_missing'
            WHEN total_laid_off IS NULL THEN 'only_total_missing'
            WHEN percentage_laid_off IS NULL THEN 'only_percentage_missing'
            ELSE 'both_present'
        END as missing_pattern,
        COUNT(*) as count
    FROM layoffs_staging2
    GROUP BY 
        stage,
        CASE 
            WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 'both_missing'
            WHEN total_laid_off IS NULL THEN 'only_total_missing'
            WHEN percentage_laid_off IS NULL THEN 'only_percentage_missing'
            ELSE 'both_present'
        END
)
SELECT 
    stage,
    missing_pattern,
    count,
    ROUND(100.0 * count / SUM(count) OVER (PARTITION BY stage), 2) as pct_within_stage,			# defining the partition by stage to then get the total counts for individual stages and using that as the denominator for the fraction calculating the relative frequency of the counts of the missing pattern within that stage.
    ROUND(100.0 * count / SUM(count) OVER (), 2) as pct_of_total								# window function over the entire column, so as to "escape" the group by statement inherited from the cte.
FROM pattern_counts
ORDER BY stage, missing_pattern;

/* A few insights based on this analysis.

Overall, it looks like "both_present" is the most common pattern across almost all stages, typically ranging from 40-60% of cases within each stage. "both_missing" instead occurs in about 10-20% of cases for most stages, as pointed out above.
Late-stage or public companies tend to disclose more comprehensively, with post-IPO companies reporting ~55% both present and only 10.33% both missing.
Early-stage companies instead generally show more missing data, although the group sizes are substantially larger than some later stage ones (e.g. Series F onwards).
Private Equity companies have surprisingly good data quality (~63% both present), while Acquired companies show worse figures than one might expect (~40% both present).

Diving deeper in the patterns of partial missing data, it looks like Seed stage companies seem more hesitant to disclose absolute numbers but more willing to give percentages, not disclosing the total number of employees laid off in ~32% of the cases (vs not disclosing percentage of total in about 11% of cases).
For Series A, B, C, and D the disclosure preferences seem rather balanced, presenting comparable relative frequencies.
Acquired companies instead tend to show a substantial asymmetry favoring missing percentages (~27%) over missing totals (~11%).
Unsurprisingly, public companies (i.e. Post-IPO) show the most balanced pattern, possibly due to more standardized reporting requirements.
*/

/* Overall, this exercise was useful to formulate some hypotheses about patterns in missing data.
Based on these results, I will create a new table and save to it the data until this point (including the rows with both missing values).
Then I will proceed to delete the 361 rows with both missing value from the new table given the goal of the following task: exploratory data analysis.
Those rows will skew the conclusions we will get from those data, unless we remove them and continue the analysis without them.
I will then proceed with the exploratory data analysis using the new table.
*/

## Create new table

CREATE TABLE `layoffs_eda` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

## Write to it all records from layoffs_staging2
INSERT layoffs_eda
SELECT *
FROM layoffs_staging2;

## Delete rows with both missing values.
DELETE
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;



# ------ Exploratory Data Analysis using layoffs_eda table.




