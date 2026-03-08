SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize Data (spelling, etc.)
-- 3. Null/Blank values
-- 4. Remove Any Columns

-- First we must duplicate the table to avoid losing raw data info
CREATE TABLE layoffs_staging
LIKE layoffs;

-- refresh
-- this only gives the attribute names
SELECT * 
FROM layoffs_staging;

-- copy all info into this new table
INSERT layoffs_staging
SELECT *
FROM layoffs;


-- This dataset has no id, so removing dupes are harder
-- date with backticks
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- create a CTE to show duplicates (row_num>1)
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;


-- checking one of them shows that these are not exact dupes!
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- need to partition over every column
-- now its accurate
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;



-- now we only want to delete one of the dupes
-- diff ways to delete them
-- in Microsoft SQL Server, can delete within CTE, but cant in mySQL
-- the way to do it is to replace SELECT * with DELETE
-- but ull get an error that duplicate_cte isnt updatable

-- so take inner bracket and put into staging 2 database
-- then filter on row_nums and delete the ones = 2


-- double click layoffs_staging, copy to clipboard, create statement, and paste below
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- insert then load again

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- error 1175 occured
-- Edit -> Preferences -> SQL Editor -> deselect Safe Updates
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- now we can get rid of row_num

-- STANDARDIZING DATA
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- notice that alot of labels are quite similar to e/o (Crypto & CryptoCurrency)
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- check the others

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%' ;

-- need to trim trail
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- changing text to date
-- capital Y means 4 number long year
-- lower case Y takes first 2 numbers
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

-- if u see domain of date, its still text
-- now we can alter into a date type
-- never do this in raw table!
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;


--  NULLS/BLANKS
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- try to populate since there're same companies with filled in industry column
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location -- if theres another airbnb in another location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- didnt work, maybe bc its a blank and not NULL
-- set to null first:
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- checking other nulls, only Bally's stayed NULL bc its unique
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- looking at where both total & % laid off is NULL, we can get rid of it, not useful
-- we can delete, but should we delete?
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- also dont need row_num anymore
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;