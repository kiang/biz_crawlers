<?php

namespace BizData\Crawlers;

use Symfony\Component\DomCrawler\Crawler;

class GCISCrawler extends BaseCrawler
{
    private const BASE_URL = 'https://serv.gcis.nat.gov.tw/pub/cmpy/reportReg.jsp';
    private const REPORT_URL = 'https://serv.gcis.nat.gov.tw/pub/cmpy/reportAction.do';
    
    private array $organizations = [];
    private array $types = [
        'S' => '設立',
        'C' => '變更', 
        'D' => '解散'
    ];
    
    public function crawl(array $params = []): array
    {
        $year = $params['year'] ?? null;
        $month = $params['month'] ?? null;
        
        if (!$year || !$month) {
            throw new \InvalidArgumentException('Year and month parameters are required');
        }
        
        // Convert Western calendar year to Taiwan ROC year if needed
        $rocYear = $this->convertToRocYear($year);
        
        $this->logger->info("Starting GCIS crawl for ROC {$rocYear}-{$month} (Western {$year}-{$month})");
        
        // First get the organizations list
        $this->fetchOrganizations();
        
        // Then crawl each organization and type combination
        $allIds = [];
        foreach ($this->organizations as $orgId => $orgName) {
            foreach ($this->types as $typeId => $typeName) {
                $ids = $this->crawlReportType($rocYear, $month, $orgId, $typeId);
                $allIds = array_merge($allIds, $ids);
                
                $this->logger->info("Found " . count($ids) . " IDs for {$orgName}-{$typeName}");
            }
        }
        
        $uniqueIds = array_unique($allIds);
        $this->logger->info("Total unique company IDs found: " . count($uniqueIds));
        
        return $uniqueIds;
    }
    
    private function fetchOrganizations(): void
    {
        $this->logger->info("Fetching organizations list");
        
        $crawler = $this->fetch(self::BASE_URL);
        
        // Convert from Big5 to UTF-8 if needed
        $content = $crawler->html();
        if (mb_detect_encoding($content, 'UTF-8', true) === false) {
            $content = mb_convert_encoding($content, 'UTF-8', 'Big5');
        }
        
        // Parse organizations from select dropdown
        $orgSelect = $crawler->filter('select[name="org"]');
        if ($orgSelect->count() > 0) {
            $orgSelect->filter('option')->each(function (Crawler $option) {
                $value = $option->attr('value');
                $text = trim($option->text());
                
                if ($value && $text && $value !== '') {
                    $this->organizations[$value] = $text;
                }
            });
        }
        
        $this->logger->info("Found " . count($this->organizations) . " organizations");
    }
    
    private function crawlReportType(int $year, int $month, string $orgId, string $typeId): array
    {
        $yearMonth = sprintf("%03d%02d", $year, $month);
        $fileName = "{$yearMonth}{$orgId}{$typeId}.pdf";
        
        $url = self::REPORT_URL . "?" . http_build_query([
            'method' => 'report',
            'reportClass' => 'cmpy',
            'subPath' => $yearMonth,
            'fileName' => $fileName
        ]);
        
        try {
            $this->logger->info("Downloading PDF: {$fileName}");
            $pdfFile = $this->downloadFile($url);
            
            $ids = $this->extractIdsFromPdf($pdfFile);
            
            // Clean up temporary file
            if (file_exists($pdfFile)) {
                unlink($pdfFile);
            }
            
            return $ids;
            
        } catch (\Exception $e) {
            $this->logger->warning("Failed to process {$fileName}: " . $e->getMessage());
            return [];
        }
    }
    
    private function extractIdsFromPdf(string $pdfFile): array
    {
        $textFile = tempnam(sys_get_temp_dir(), 'pdf_text_');
        
        try {
            // Convert PDF to text using pdftotext
            $command = sprintf(
                'pdftotext -enc UTF-8 %s %s 2>/dev/null',
                escapeshellarg($pdfFile),
                escapeshellarg($textFile)
            );
            
            exec($command, $output, $returnCode);
            
            if ($returnCode !== 0 || !file_exists($textFile)) {
                throw new \Exception("Failed to convert PDF to text");
            }
            
            $content = file_get_contents($textFile);
            
            // Extract 8-digit company IDs
            preg_match_all('/\d{8}/', $content, $matches);
            
            $ids = array_unique($matches[0]);
            
            $this->logger->info("Extracted " . count($ids) . " company IDs from PDF");
            
            return $ids;
            
        } finally {
            // Clean up temporary text file
            if (file_exists($textFile)) {
                unlink($textFile);
            }
        }
    }
    
    public function crawlBusiness(int $year, int $month): array
    {
        // Convert Western calendar year to Taiwan ROC year if needed
        $rocYear = $this->convertToRocYear($year);
        
        $this->logger->info("Starting GCIS business crawl for ROC {$rocYear}-{$month} (Western {$year}-{$month})");
        
        // Get business organizations from different URL
        $businessUrl = 'https://serv.gcis.nat.gov.tw/moeadsBF/bms/report.jsp';
        $crawler = $this->fetch($businessUrl);
        
        // Convert encoding if needed
        $content = $crawler->html();
        if (mb_detect_encoding($content, 'UTF-8', true) === false) {
            $content = mb_convert_encoding($content, 'UTF-8', 'Big5');
        }
        
        $organizations = [];
        $areaSelect = $crawler->filter('select[name="area"]');
        if ($areaSelect->count() > 0) {
            $areaSelect->filter('option')->each(function (Crawler $option) use (&$organizations) {
                $value = $option->attr('value');
                $text = trim($option->text());
                
                if ($value && $text && $value !== '') {
                    $organizations[$value] = $text;
                }
            });
        }
        
        $businessTypes = [
            'setup' => '設立',
            'change' => '變更',
            'rest' => '解散'
        ];
        
        $allIds = [];
        foreach ($organizations as $orgId => $orgName) {
            foreach ($businessTypes as $typeId => $typeName) {
                $ids = $this->crawlBusinessReportType($rocYear, $month, $orgId, $typeId);
                $allIds = array_merge($allIds, $ids);
                
                $this->logger->info("Found " . count($ids) . " business IDs for {$orgName}-{$typeName}");
            }
        }
        
        $uniqueIds = array_unique($allIds);
        $this->logger->info("Total unique business IDs found: " . count($uniqueIds));
        
        return $uniqueIds;
    }
    
    private function crawlBusinessReportType(int $year, int $month, string $orgId, string $typeId): array
    {
        $yearMonth = sprintf("%03d%02d", $year, $month);
        $fileName = "{$orgId}{$typeId}{$yearMonth}.pdf";
        
        $url = "https://serv.gcis.nat.gov.tw/moeadsBF/cmpy/reportAction.do?" . http_build_query([
            'method' => 'report',
            'reportClass' => 'bms',
            'subPath' => $yearMonth,
            'fileName' => $fileName
        ]);
        
        try {
            $this->logger->info("Downloading business PDF: {$fileName}");
            $pdfFile = $this->downloadFile($url);
            
            $ids = $this->extractIdsFromPdf($pdfFile);
            
            // Clean up temporary file
            if (file_exists($pdfFile)) {
                unlink($pdfFile);
            }
            
            return $ids;
            
        } catch (\Exception $e) {
            $this->logger->warning("Failed to process business {$fileName}: " . $e->getMessage());
            return [];
        }
    }
    
    /**
     * Convert Western calendar year to Taiwan ROC year
     * Provides helpful error messages for common mistakes
     */
    private function convertToRocYear(int $year): int
    {
        // If year is already in ROC format (reasonable range: 100-150)
        if ($year >= 100 && $year <= 150) {
            return $year;
        }
        
        // If year is in Western format (2000+), convert to ROC
        if ($year >= 2000) {
            $rocYear = $year - 1911;
            $this->logger->info("Converted Western year {$year} to ROC year {$rocYear}");
            return $rocYear;
        }
        
        // If year seems incorrect, provide helpful error
        if ($year < 100) {
            throw new \InvalidArgumentException(
                "Year {$year} seems too small. Please use either:\n" .
                "- ROC calendar year (e.g., 114 for 2025)\n" .
                "- Western calendar year (e.g., 2025, will be converted to ROC 114)"
            );
        }
        
        if ($year > 1911 && $year < 2000) {
            throw new \InvalidArgumentException(
                "Year {$year} is ambiguous. Please use either:\n" .
                "- ROC calendar year (e.g., 114 for 2025)\n" .
                "- Western calendar year (e.g., 2025, will be converted to ROC 114)"
            );
        }
        
        // Fallback - assume it's already ROC year
        return $year;
    }
}