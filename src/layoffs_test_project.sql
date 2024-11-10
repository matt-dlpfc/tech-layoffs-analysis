USE layoffs_data;

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

