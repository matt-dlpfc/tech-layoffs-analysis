USE layoffs_data;

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

## Find and decide what to do about "substantially missing" data.
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL AND total_laid_off IS NULL;

/* 361 rows seem to have both columns as NULL (no empty cells found).
By quickly looking at the stage, it doesn't seem that the distribution of NULL values is random.
I will check this by comparing the relative distribution of null values by stage, partitioned by whether they are BOTH NULL or else.
*/
## Check and compare the distribution of missing values across stages.

WITH null_cte AS (
	SELECT *, CASE
		WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 'null'
		ELSE 'not null' END AS check_if_null
	FROM layoffs_staging2
    )
SELECT stage, COUNT(stage)
FROM null_cte
GROUP BY stage
ORDER BY stage; # Need to continue...

SELECT *, CASE
	WHEN total_laid_off IS NULL AND percentage_laid_off IS NULL THEN 'null'
    ELSE 'not null' END AS check_if_null
FROM layoffs_staging2;


