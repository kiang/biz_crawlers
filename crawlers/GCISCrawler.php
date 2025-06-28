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
    private array $processedReports = [];
    private array $processedPdfs = [];
    private array $processedFilenames = [];

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

        // Load processed PDFs to avoid duplicates
        $this->loadProcessedPdfs();

        // First get the organizations list
        $this->fetchOrganizations();

        // Then crawl each organization and type combination
        $allIds = [];
        $typeResults = []; // Store results by type to save consolidated data

        foreach ($this->organizations as $orgId => $orgName) {
            foreach ($this->types as $typeId => $typeName) {
                $ids = $this->crawlReportType($rocYear, $month, $orgId, $typeId);
                $allIds = array_merge($allIds, $ids);

                // Collect IDs by type for consolidated saving
                $categoryMap = [
                    'S' => 'establishments',
                    'C' => 'changes',
                    'D' => 'dissolutions'
                ];
                $category = $categoryMap[$typeId] ?? $typeId;

                if (!isset($typeResults[$category])) {
                    $typeResults[$category] = [];
                }
                $typeResults[$category] = array_merge($typeResults[$category], $ids);

                $this->logger->info("Found " . count($ids) . " IDs for {$orgName}-{$typeName}");
            }
        }

        // Save consolidated data by type
        foreach ($typeResults as $category => $ids) {
            if (!empty($ids)) {
                $this->saveDataToRepository('gcis', 'companies', $category, $rocYear, $month, $ids);
            }
        }

        // Save processed PDFs tracking
        $this->saveProcessedPdfs();

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

        // Check for duplicate based on filename BEFORE any download
        if ($this->isFilenameAlreadyProcessed($fileName)) {
            $this->logger->info("PDF {$fileName} already processed, loading existing data");
            return $this->loadExistingPdfDataByFilename($fileName);
        }

        // Check if we already downloaded this specific report
        $reportKey = "{$orgId}_{$typeId}_{$year}_{$month}";
        if ($this->isReportProcessed($reportKey)) {
            $this->logger->info("Report {$fileName} already processed, skipping");
            return [];
        }

        $url = self::REPORT_URL . "?" . http_build_query([
            'method' => 'report',
            'reportClass' => 'cmpy',
            'subPath' => $yearMonth,
            'fileName' => $fileName
        ]);

        try {
            $this->logger->info("Downloading PDF: {$fileName}");
            $pdfFile = $this->downloadFile($url);

            // Generate unique ID for this PDF
            $uniqueId = $this->generateUniqueIdForPdf($pdfFile, $fileName);

            $ids = $this->extractIdsFromPdf($pdfFile);

            // Save individual PDF results with unique ID
            $this->savePdfData($uniqueId, $fileName, $ids, ['org' => $orgId, 'type' => $typeId, 'year' => $year, 'month' => $month]);

            // Mark PDF as processed
            $this->markPdfProcessed($uniqueId, $fileName);
            $this->markFilenameProcessed($fileName);

            // Clean up temporary file
            if (file_exists($pdfFile)) {
                unlink($pdfFile);
            }

            // Mark this report as processed
            $this->markReportProcessed($reportKey);

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

        // Load processed PDFs to avoid duplicates
        $this->loadProcessedPdfs();

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

        // Save processed PDFs tracking
        $this->saveProcessedPdfs();

        $uniqueIds = array_unique($allIds);
        $this->logger->info("Total unique business IDs found: " . count($uniqueIds));

        return $uniqueIds;
    }

    private function crawlBusinessReportType(int $year, int $month, string $orgId, string $typeId): array
    {
        // Map type ID to category name
        $categoryMap = [
            'setup' => 'establishments',
            'change' => 'changes',
            'rest' => 'dissolutions'
        ];
        $category = $categoryMap[$typeId] ?? $typeId;

        // Check if we already have this report
        if ($this->checkReportExists('gcis', 'businesses', $category, $year, $month)) {
            $this->logger->info("Business report already exists for {$category} {$year}-{$month}, loading existing data");
            return $this->loadExistingData('gcis', 'businesses', $category, $year, $month);
        }

        $yearMonth = sprintf("%03d%02d", $year, $month);
        $fileName = "{$orgId}{$typeId}{$yearMonth}.pdf";

        // Check for duplicate based on filename BEFORE any download
        if ($this->isFilenameAlreadyProcessed($fileName)) {
            $this->logger->info("Business PDF {$fileName} already processed, loading existing data");
            return $this->loadExistingPdfDataByFilename($fileName);
        }

        $url = "https://serv.gcis.nat.gov.tw/moeadsBF/cmpy/reportAction.do?" . http_build_query([
            'method' => 'report',
            'reportClass' => 'bms',
            'subPath' => $yearMonth,
            'fileName' => $fileName
        ]);

        try {
            $this->logger->info("Downloading business PDF: {$fileName}");
            $pdfFile = $this->downloadFile($url);

            // Generate unique ID for this PDF
            $uniqueId = $this->generateUniqueIdForPdf($pdfFile, $fileName);

            // Check for duplicate based on filename before processing
            if ($this->isFilenameAlreadyProcessed($fileName)) {
                $this->logger->info("Business PDF {$fileName} already processed, loading existing data");
                if (file_exists($pdfFile)) {
                    unlink($pdfFile);
                }
                return $this->loadExistingPdfDataByFilename($fileName);
            }

            $ids = $this->extractIdsFromPdf($pdfFile);

            // Save individual PDF results with unique ID
            $this->savePdfData($uniqueId, $fileName, $ids, ['org' => $orgId, 'type' => $typeId, 'year' => $year, 'month' => $month, 'business_type' => true]);

            // Mark PDF as processed
            $this->markPdfProcessed($uniqueId, $fileName);
            $this->markFilenameProcessed($fileName);

            // Clean up temporary file
            if (file_exists($pdfFile)) {
                unlink($pdfFile);
            }

            // Save the data to repository
            if (!empty($ids)) {
                $this->saveDataToRepository('gcis', 'businesses', $category, $year, $month, $ids);
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

    private function isReportProcessed(string $reportKey): bool
    {
        return in_array($reportKey, $this->processedReports);
    }

    private function markReportProcessed(string $reportKey): void
    {
        $this->processedReports[] = $reportKey;
    }

    private function generateUniqueIdForPdf(string $pdfFile, string $fileName): string
    {
        $fileHash = hash_file('sha256', $pdfFile);
        $shortHash = substr($fileHash, 0, 8);
        $timestamp = date('Ymd_His');
        $baseName = pathinfo($fileName, PATHINFO_FILENAME);

        return "{$baseName}_{$timestamp}_{$shortHash}";
    }

    private function isFilenameAlreadyProcessed(string $fileName): bool
    {
        return in_array($fileName, $this->processedFilenames);
    }

    private function markPdfProcessed(string $uniqueId, string $fileName): void
    {
        $this->processedPdfs[$uniqueId] = [
            'filename' => $fileName,
            'processed_at' => date('c')
        ];
    }

    private function markFilenameProcessed(string $fileName): void
    {
        if (!in_array($fileName, $this->processedFilenames)) {
            $this->processedFilenames[] = $fileName;
        }
    }

    private function savePdfData(string $uniqueId, string $fileName, array $ids, array $metadata): void
    {
        $dataDir = dirname(__DIR__) . '/data/gcis/pdfs';
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }

        $filePath = "{$dataDir}/{$uniqueId}.txt";
        file_put_contents($filePath, implode("\n", $ids));

        $this->logger->info("Saved " . count($ids) . " IDs from {$fileName} to {$uniqueId}.txt");

        // Also save metadata
        $metadataFile = "{$dataDir}/{$uniqueId}_metadata.json";
        $metadataContent = [
            'unique_id' => $uniqueId,
            'original_filename' => $fileName,
            'processed_at' => date('c'),
            'ids_count' => count($ids),
            'metadata' => $metadata
        ];
        file_put_contents($metadataFile, json_encode($metadataContent, JSON_PRETTY_PRINT));
    }

    private function loadExistingPdfDataByFilename(string $fileName): array
    {
        $dataDir = dirname(__DIR__) . '/data/gcis/pdfs';

        // Look for existing file with same filename
        foreach ($this->processedPdfs as $existingId => $data) {
            if ($data['filename'] === $fileName) {
                $filePath = "{$dataDir}/{$existingId}.txt";
                if (file_exists($filePath)) {
                    $content = file_get_contents($filePath);
                    return array_filter(explode("\n", $content));
                }
            }
        }

        return [];
    }

    private function loadProcessedPdfs(): void
    {
        $trackingFile = dirname(__DIR__) . '/data/gcis/processed_pdfs.json';

        if (file_exists($trackingFile)) {
            $data = json_decode(file_get_contents($trackingFile), true);
            $this->processedPdfs = $data ?: [];

            // Build filename array for quick lookup
            $this->processedFilenames = [];
            foreach ($this->processedPdfs as $uniqueId => $data) {
                if (!in_array($data['filename'], $this->processedFilenames)) {
                    $this->processedFilenames[] = $data['filename'];
                }
            }
        }
    }

    private function saveProcessedPdfs(): void
    {
        $dataDir = dirname(__DIR__) . '/data/gcis';
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }

        $trackingFile = "{$dataDir}/processed_pdfs.json";
        file_put_contents($trackingFile, json_encode($this->processedPdfs, JSON_PRETTY_PRINT));
    }

    public function saveIdList(string $type, int $year, int $month, array $ids): string
    {
        $rocYear = $this->convertToRocYear($year);
        // Create directory with ROC year but filename with original year for consistency
        $dataDir = dirname(__DIR__) . '/data';
        $sourceDir = "{$dataDir}/gcis/{$type}";
        $yearMonthDir = sprintf("%s/%03d-%02d", $sourceDir, $rocYear, $month);
        
        if (!is_dir($yearMonthDir)) {
            mkdir($yearMonthDir, 0755, true);
        }
        
        $filename = "ids_{$type}_{$year}_{$month}.txt";
        $filepath = "{$yearMonthDir}/{$filename}";
        
        // Always create the file, even if empty
        file_put_contents($filepath, implode("\n", $ids));
        
        return $filepath;
    }
}
