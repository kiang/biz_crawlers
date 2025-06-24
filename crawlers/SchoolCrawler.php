<?php

namespace BizData\Crawlers;

use Symfony\Component\DomCrawler\Crawler;

class SchoolCrawler extends BaseCrawler
{
    private const SCHOOL_LIST_URL = 'http://140.111.34.54/GENERAL/school_list.aspx';
    private const SOURCE_URL = 'http://140.111.34.54/GENERAL/index.aspx';
    
    public function crawl(array $params = []): array
    {
        $this->logger->info("Starting school data crawl");
        
        $allSchools = [];
        $pageNumber = 0;
        
        while (true) {
            $this->logger->info("Crawling page {$pageNumber}");
            
            $schools = $this->crawlPage($pageNumber);
            
            if (empty($schools)) {
                $this->logger->info("No more schools found on page {$pageNumber}, stopping");
                break;
            }
            
            $allSchools = array_merge($allSchools, $schools);
            $this->logger->info("Found " . count($schools) . " schools on page {$pageNumber}");
            
            $pageNumber++;
            
            // Add delay between pages to be respectful
            if ($this->config['delay'] > 0) {
                sleep($this->config['delay']);
            }
        }
        
        $this->logger->info("Total schools crawled: " . count($allSchools));
        
        return $allSchools;
    }
    
    private function crawlPage(int $pageNumber): array
    {
        $url = self::SCHOOL_LIST_URL . '?' . http_build_query([
            'pages' => $pageNumber,
            'site_content_sn' => '16678'
        ]);
        
        try {
            $crawler = $this->fetch($url);
            
            // Find the table with school information
            $schoolTable = null;
            $crawler->filter('table')->each(function (Crawler $table) use (&$schoolTable) {
                if ($table->attr('summary') === '訊息列表') {
                    $schoolTable = $table;
                }
            });
            
            if (!$schoolTable) {
                $this->logger->warning("Could not find school table on page {$pageNumber}");
                return [];
            }
            
            $schools = [];
            $isEmpty = true;
            
            $schoolTable->filter('tr')->each(function (Crawler $row) use (&$schools, &$isEmpty) {
                $class = $row->attr('class');
                
                if (!in_array($class, ['td_style01', 'td_style02'])) {
                    return; // Skip rows that don't contain school data
                }
                
                $isEmpty = false;
                
                $cells = $row->filter('td');
                if ($cells->count() >= 4) {
                    $school = $this->parseSchoolRow($cells);
                    if ($school) {
                        $schools[] = $school;
                    }
                }
            });
            
            // If the page is empty, we've reached the end
            if ($isEmpty) {
                return [];
            }
            
            return $schools;
            
        } catch (\Exception $e) {
            $this->logger->error("Failed to crawl page {$pageNumber}: " . $e->getMessage());
            return [];
        }
    }
    
    private function parseSchoolRow(Crawler $cells): ?array
    {
        try {
            // Extract data from table cells
            $category = $this->extractText($cells->eq(1), '');
            $taxId = $this->extractText($cells->eq(2), '');
            $name = $this->extractText($cells->eq(3), '');
            
            // Validate tax ID
            if (!$this->isValidTaxId($taxId)) {
                $this->logger->warning("Invalid tax ID found: {$taxId}");
                return null;
            }
            
            $school = [
                'tax_id' => $taxId,
                'name' => $name,
                'category' => $category,
                'source' => self::SOURCE_URL,
                'type' => 4, // School type identifier
                'crawled_at' => date('Y-m-d H:i:s')
            ];
            
            $this->logger->debug("Parsed school: " . json_encode($school));
            
            return $school;
            
        } catch (\Exception $e) {
            $this->logger->warning("Failed to parse school row: " . $e->getMessage());
            return null;
        }
    }
    
    private function isValidTaxId(string $taxId): bool
    {
        // Check if it's exactly 8 digits
        return preg_match('/^\d{8}$/', $taxId) === 1;
    }
    
    public function getSchoolByTaxId(string $taxId): ?array
    {
        $this->logger->info("Searching for school with tax ID: {$taxId}");
        
        $allSchools = $this->crawl();
        
        foreach ($allSchools as $school) {
            if ($school['tax_id'] === $taxId) {
                $this->logger->info("Found school: " . $school['name']);
                return $school;
            }
        }
        
        $this->logger->info("School with tax ID {$taxId} not found");
        return null;
    }
    
    public function getSchoolsByCategory(string $category): array
    {
        $this->logger->info("Searching for schools in category: {$category}");
        
        $allSchools = $this->crawl();
        $matchingSchools = [];
        
        foreach ($allSchools as $school) {
            if (stripos($school['category'], $category) !== false) {
                $matchingSchools[] = $school;
            }
        }
        
        $this->logger->info("Found " . count($matchingSchools) . " schools in category: {$category}");
        return $matchingSchools;
    }
    
    public function exportToCSV(string $filename = 'schools.csv'): bool
    {
        $this->logger->info("Exporting schools to CSV: {$filename}");
        
        $schools = $this->crawl();
        
        if (empty($schools)) {
            $this->logger->warning("No schools to export");
            return false;
        }
        
        $handle = fopen($filename, 'w');
        if (!$handle) {
            $this->logger->error("Failed to open file for writing: {$filename}");
            return false;
        }
        
        // Write CSV header
        $headers = ['tax_id', 'name', 'category', 'source', 'type', 'crawled_at'];
        fputcsv($handle, $headers);
        
        // Write school data
        foreach ($schools as $school) {
            $row = [
                $school['tax_id'],
                $school['name'],
                $school['category'],
                $school['source'],
                $school['type'],
                $school['crawled_at']
            ];
            fputcsv($handle, $row);
        }
        
        fclose($handle);
        
        $this->logger->info("Successfully exported " . count($schools) . " schools to {$filename}");
        return true;
    }
}