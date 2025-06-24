# Business Data Crawlers

Modern PHP web crawlers built with Goutte library to replace the original Taiwan company data crawlers.

## Features

- **Modern Architecture**: Built with Goutte (Symfony DomCrawler + Guzzle HTTP)
- **Robust Error Handling**: Automatic retries, exponential backoff, comprehensive logging
- **Flexible Configuration**: Configurable timeouts, delays, proxies, and retry policies
- **Memory Efficient**: Streaming data processing for large datasets
- **PSR-4 Compliant**: Modern PHP autoloading and namespacing

## Installation

```bash
composer install
```

## Available Crawlers

### 1. GCIS Crawler (`GCISCrawler`)
Crawls company and business data from Taiwan's GCIS (Government Company Information Service).

**Sources:**
- Company Registry: `https://serv.gcis.nat.gov.tw/pub/cmpy/reportReg.jsp`
- Business Registry: `https://serv.gcis.nat.gov.tw/moeadsBF/bms/report.jsp`

**Features:**
- Downloads PDF reports by month and organization
- Extracts 8-digit company/business IDs using `pdftotext`
- Supports both company and business data types

### 2. Tax Crawler (`TaxCrawler`)
Crawls tax registration data from Taiwan's Ministry of Finance.

**Source:**
- FIA Tax Data: `https://eip.fia.gov.tw/data/BGMOPEN1.zip`

**Features:**
- Downloads and processes daily tax registration updates
- Handles large CSV files with streaming processing
- Detects file changes to avoid unnecessary processing
- Parses industry codes and establishment dates

### 3. School Crawler (`SchoolCrawler`)
Crawls school data from Taiwan's Ministry of Education.

**Source:**
- MOE School Directory: `http://140.111.34.54/GENERAL/school_list.aspx`

**Features:**
- Paginated crawling of all registered schools
- Extracts tax IDs, names, and categories
- Export functionality to CSV format

## Usage

### Unified Command Interface

```bash
# GCIS Company Data
php crawl.php gcis --company --year 2024 --month 6

# GCIS Business Data  
php crawl.php gcis --business --year 2024 --month 6

# Tax Data (download latest)
php crawl.php tax

# Tax Data (process existing file)
php crawl.php tax --file bgmopen1.csv

# School Data (all schools)
php crawl.php school

# School Data (export to CSV)
php crawl.php school --csv schools.csv

# School Data (filter by category)
php crawl.php school --category "大學"

# School Data (search by tax ID)
php crawl.php school --tax-id 12345678
```

### Individual Crawler Scripts

```bash
# GCIS Crawler
php crawl-gcis.php company 2024 6
php crawl-gcis.php business 2024 6

# Tax Crawler
php crawl-tax.php
php crawl-tax.php bgmopen1.csv

# School Crawler
php crawl-schools.php
php crawl-schools.php --csv schools.csv
php crawl-schools.php --category "大學"
php crawl-schools.php --tax-id 12345678
```

### Programmatic Usage

```php
<?php
require_once 'vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

$factory = new CrawlerFactory();

// GCIS Crawler
$gcisCrawler = $factory->createGCISCrawler();
$companyIds = $gcisCrawler->crawl(['year' => 2024, 'month' => 6]);
$businessIds = $gcisCrawler->crawlBusiness(2024, 6);

// Tax Crawler
$taxCrawler = $factory->createTaxCrawler();
$taxData = $taxCrawler->crawl(); // Downloads latest data

// School Crawler
$schoolCrawler = $factory->createSchoolCrawler();
$schools = $schoolCrawler->crawl();
$schoolCrawler->exportToCSV('schools.csv');
```

## Configuration Options

All crawlers support these configuration options:

```php
$config = [
    'timeout' => 30,                    // HTTP request timeout (seconds)
    'delay' => 1,                       // Delay between requests (seconds)
    'retries' => 3,                     // Number of retry attempts
    'user_agent' => 'BizData Crawler/1.0', // HTTP User-Agent header
    'proxy' => null,                    // HTTP proxy URL (or use PROXY_URL env var)
    'log_file' => 'crawler.log',        // Log file path
    'log_level' => Logger::INFO         // Logging level
];

$factory = new CrawlerFactory($config);
```

## Global Command Options

```bash
--help              Show help message
--verbose           Enable debug logging
--delay <seconds>   Delay between requests
--timeout <seconds> Request timeout
--retries <count>   Number of retry attempts
```

## Dependencies

- **fabpot/goutte**: Web scraper built on Symfony DomCrawler
- **guzzlehttp/guzzle**: HTTP client library
- **monolog/monolog**: Logging library
- **pdftotext**: System utility for PDF text extraction

## Requirements

- PHP 7.4+
- `pdftotext` utility (usually provided by `poppler-utils` package)
- Composer for dependency management

## Comparison with Original Crawlers

| Feature | Original | New (Goutte-based) |
|---------|----------|-------------------|
| HTTP Client | cURL | Guzzle |
| HTML Parsing | Manual regex | Symfony DomCrawler |
| Error Handling | Basic | Comprehensive with retries |
| Logging | Basic | Monolog with levels |
| Configuration | Hardcoded | Flexible configuration |
| Memory Usage | High | Optimized streaming |
| Code Structure | Procedural | OOP with PSR-4 |
| Testing | None | Unit test ready |

## Output Files

- **GCIS Crawler**: `ids_company_YYYY_MM.txt`, `ids_business_YYYY_MM.txt`
- **Tax Crawler**: JSON sample files and processing logs
- **School Crawler**: `schools.csv` (optional export)
- **Logs**: `{type}_crawl_YYYY-MM-DD_HH-MM-SS.log`

## Error Handling

- Automatic retry with exponential backoff
- Comprehensive error logging
- Graceful handling of network timeouts
- File validation and cleanup
- Memory-efficient processing for large datasets

## License

BSD License (same as original crawler project)