# ------ Exploratory Data Analysis using layoffs_eda table.
/* I believe in curiosity-driven education. 
Therefore, I will follow my curiosity to determine next steps 
of the analysis, until it "saturates" with something useful. 
*/

## How have different industries been affected by layoffs?
SELECT 
	industry, 
    YEAR(`date`) AS yyear,
    SUM(total_laid_off) AS sum, 
    COUNT(total_laid_off) AS count,
    ROUND(AVG(total_laid_off),0) AS avg_laid_off,
    ROUND(AVG(percentage_laid_off)*100,2) AS avg_pct_laid_off,
    ROUND(AVG(total_laid_off)*100/(AVG(percentage_laid_off)*100),0) AS avg_company_size_before_layoff
FROM layoffs_eda 
# WHERE industry = 'Other'
GROUP BY industry, yyear
ORDER BY 2 DESC, 3 DESC;

/* What are the companies in industry = 'Other'? 
Looks odd that in 2023 that has been the category with the most layoffs
*/

## Investigating "the others" - Who are they?
SELECT *, 
	SUM(total_laid_off) OVER(PARTITION BY YEAR(`date`) ORDER BY total_laid_off ASC)
FROM layoffs_eda
WHERE industry = 'Other' AND `date` IS NOT NULL
ORDER BY YEAR(`date`) ASC, total_laid_off DESC;

/* Weirdly enough, they seem to include many BigTech or Tech companies such as 
Microsoft, Twilio, Automation Anywhere, Indigo, Hopin, Asana, SAP, Ericsson, Thoughtworks.
*/

## What are the most common months, weeks, days to announce layoffs (across all industries)?
SELECT
	MONTH(`date`) AS mmonth,
    SUM(total_laid_off) AS sum,
    SUM(total_laid_off) - ROUND((SELECT SUM(total_laid_off)/52 FROM layoffs_eda),0) AS dev_from_global_weekly_avg_sum
FROM layoffs_eda
WHERE `date` IS NOT NULL
GROUP BY mmonth
ORDER BY sum DESC;

/* Across the period 2020-2023, the most common month for layoffs has been January (92K people laid off),
followed by November (~56K) and February (~41K).
The least common months have been September (~6.6K) and December (~12K).
I guess the spirit is: 
1. let people enjoy Christimas and New Year's Eve times; 
2. try to have them hate the company a little bit less when firing them in November so that they have a tiny chance 
	to find another job by the end of the year, or use that time to relax and start the new year afresh;
3. wish them "Happy New Year and new you... yes, new you; goodbye and good luck!" and have mass layoffs in January (around 1/4)
	so that they can use their newly gained energy for something as fun and exciting as jub hunting! 
    (hopefully the ironic tone is not too subtle)
 */

SELECT
	WEEK(`date`) AS wweek,
    SUM(total_laid_off) AS sum
FROM layoffs_eda
WHERE `date` IS NOT NULL
GROUP BY wweek
ORDER BY sum DESC;

/* What's even more curious is to observe this data at the week level. 
~34K people have been laid off in week 3, making it the most commond announcement week.
The second place goes to week 46 (~21K), and then even more sadly, following closely along, week 1 (~20.5K).
Week 2 instead seems not to be a desirable week, for some reason, reaching the 17th place (out of 52 possible places).
The least common week, by far, was the last week of the year, week 52 (150 people), while week 51 occupies the 48th place (with ~1.2K).

<begin rant>
I am curious as to whose companies' management teams concluded it was a "kind" thing to have group layoffs in those two weeks, when
	the affected employees and their families could only "suck it" for those weeks, since they wouldn't be able start seeking 
    another job right away, because everyone is on holiday, thus making these people feel helpless and anxious...
If you have to fire someone, do it when they have the chance to act on it immediately: do it on Mondays, and avoid holiday weeks!
</end rant>
*/

## Which have had these "brilliant and kind" management teams?
SELECT WEEK(`date`), company, location, total_laid_off, `date`, funds_raised_millions
FROM layoffs_eda
WHERE WEEK(`date`) >=51;

/* Interesting names... BackMarket (I am a customer, actually...), Delivery Hero, Pulse Secure, Qualcomm, and some more... 
not cool!
*/


## More stuff... industry, funds raised, countries and location questions...
