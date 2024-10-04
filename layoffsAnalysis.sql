SELECT *
FROM layoffs;
-- created a layoffs staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- removing duplicates
SELECT *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as RowNum
FROM layoffs_staging;

With duplicate_cte as
(
SELECT *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as RowNum
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
Where RowNum > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- deleting duplicates
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
(
SELECT *, row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as RowNum
FROM layoffs_staging
);

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- Standardizing data
SELECT TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE '%crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE '%United States%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE '%United States%';

SELECT date, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- hanadling nulls
SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL AND percentage_laid_off is NULL;

SELECT *
FROM layoffs_staging2
WHERE industry is NULL OR industry = '';

UPDATE layoffs_staging2
SET industry = NUll 
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1 JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL) AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1 JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL) AND t2.industry IS NOT NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off is NULL AND percentage_laid_off is NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num