select*
from layoffs;
-- 1. Removing Duplicates
-- 2. Standardize data
-- 3. Null values
-- 4. remove any columns

create table layoffs_staging
like layoffs;

select*
from layoffs_staging;

insert layoffs_staging
select*
from layoffs;




-- 1. removing duplicates

select* ,
ROW_NUMBER () over(partition by company,industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;


-- cte

with duplicate_cte as
(
select*,
row_number() over (partition by company,location,industry, total_laid_off, percentage_laid_off, 'date',stage ,country, funds_raised_millions) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;




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


insert into layoffs_staging2
select*,
row_number() over (partition by company,location,industry, total_laid_off, percentage_laid_off, 'date',stage ,country, funds_raised_millions) as row_num
from layoffs_staging;


select* 
from layoffs_staging2;

delete
from layoffs_staging2
where row_num >1;


SET SQL_SAFE_UPDATES = 0;





-- 2. Standardizing data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct(industry)
from layoffs_staging2
order by 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry = 'crypto';


select distinct(location)
from layoffs_staging2
order by 1;

select distinct(country)
from layoffs_staging2
order by 1;


select distinct(country),trim(trailing '.' from country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country = trim(trailing '.' from country);


select `date`,
trim(str_to_date(`date`, '%m/%d/%Y'))
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;




-- removing null values




select*
from layoffs_staging2
where total_laid_off is NULL
and percentage_laid_off is null;

select*
from layoffs_staging2
where industry is null
or industry = '';

select*
from layoffs_staging2
where company = "Bally's Interactive";

update layoffs_staging2
set industry = null
where industry = '';


select*
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;



-- removing columns/rows


delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging2;















