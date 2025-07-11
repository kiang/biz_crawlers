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

        // Initialize the organizations list
        $this->initializeOrganizations();

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

        // If we got 0 IDs, try cleaning inconsistent processed PDFs and retry once
        if (empty($uniqueIds) && !isset($params['_retry'])) {
            $this->logger->warning("Found 0 IDs, checking for inconsistent processed PDFs...");
            $cleaned = $this->cleanInconsistentProcessedPdfs($rocYear, $month);
            if ($cleaned > 0) {
                $this->logger->info("Cleaned {$cleaned} inconsistent entries, retrying crawl...");
                $params['_retry'] = true; // Prevent infinite recursion
                return $this->crawl($params);
            }
        }

        return $uniqueIds;
    }

    private function initializeOrganizations(): void
    {
        // Static list of GCIS organizations (fetched 2025-06-28)
        $this->organizations = [
            'AL' => '全國不分區',
            'MO' => '經濟部商業發展署',
            'CT' => '經濟部商業發展署(南投辦公區)',
            'DO' => '臺北市商業處',
            'NT' => '新北市政府經濟發展局',
            'TY' => '桃園市政府經濟發展局',
            'TC' => '臺中市政府經濟發展局',
            'KC' => '高雄市政府經濟發展局',
            'TN' => '臺南市政府經濟發展局',
            'SI' => '科學園區管理區',
            'CS' => '中部科學園區管理局',
            'ST' => '南部科學園區管理局',
            'EP' => '經濟部產業園區管理局',
            'PT' => '農業部農業科技園區管理中心'
        ];

        $this->logger->info("Initialized " . count($this->organizations) . " organizations");
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

        // Static list of business areas (fetched 2025-06-28)
        $organizations = [
            '376570000A' => '基隆市政府',
            '376410000A' => '新北市政府',
            '379100000G' => '台北市政府',
            '376430000A' => '桃園市政府',
            '376440000A' => '新竹縣政府',
            '376580000A' => '新竹市政府',
            '376450000A' => '苗栗縣政府',
            '376460000A' => '台中縣政府',
            '376590000A' => '台中市政府',
            '376480000A' => '南投縣政府',
            '376470000A' => '彰化縣政府',
            '376490000A' => '雲林縣政府',
            '376500000A' => '嘉義縣政府',
            '376600000A' => '嘉義市政府',
            '376510000A' => '台南縣政府',
            '376610000A' => '台南市政府',
            '376520000A' => '高雄縣政府',
            '383100000G' => '高雄市政府',
            '376530000A' => '屏東縣政府',
            '376420000A' => '宜蘭縣政府',
            '376550000A' => '花蓮縣政府',
            '376540000A' => '台東縣政府',
            '376560000A' => '澎湖縣政府',
            '371010000A' => '金門縣政府',
            '371030000A' => '連江縣政府'
        ];

        $this->logger->info("Initialized " . count($organizations) . " business areas");

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

        // If we got 0 IDs, try cleaning inconsistent processed PDFs and retry once
        if (empty($uniqueIds) && !isset($this->businessRetryFlag)) {
            $this->logger->warning("Found 0 business IDs, checking for inconsistent processed PDFs...");
            $cleaned = $this->cleanInconsistentBusinessPdfs($rocYear, $month);
            if ($cleaned > 0) {
                $this->logger->info("Cleaned {$cleaned} inconsistent business entries, retrying crawl...");
                $this->businessRetryFlag = true; // Prevent infinite recursion
                $result = $this->crawlBusiness($year, $month);
                unset($this->businessRetryFlag); // Clean up flag
                return $result;
            }
        }

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

    private function cleanInconsistentProcessedPdfs(int $rocYear, int $month): int
    {
        $yearMonth = sprintf("%03d%02d", $rocYear, $month);
        $dataDir = dirname(__DIR__) . '/data/gcis/pdfs';
        $cleaned = 0;
        $toRemove = [];

        // Check for inconsistent entries for this specific year/month
        foreach ($this->processedPdfs as $id => $info) {
            $filename = $info['filename'];
            
            // Check if this is a company PDF for the target year/month
            if (preg_match('/^' . preg_quote($yearMonth) . '/', $filename)) {
                $txtFile = "{$dataDir}/{$id}.txt";
                if (!file_exists($txtFile)) {
                    $toRemove[] = $id;
                    $this->logger->info("Found inconsistent entry: {$filename} (missing {$id}.txt)");
                }
            }
        }

        // Remove inconsistent entries
        foreach ($toRemove as $id) {
            unset($this->processedPdfs[$id]);
            $cleaned++;
        }

        // Save updated tracking file if we cleaned anything
        if ($cleaned > 0) {
            $this->saveProcessedPdfs();
        }

        return $cleaned;
    }

    private function cleanInconsistentBusinessPdfs(int $rocYear, int $month): int
    {
        $yearMonth = sprintf("%03d%02d", $rocYear, $month);
        $dataDir = dirname(__DIR__) . '/data/gcis/pdfs';
        $cleaned = 0;
        $toRemove = [];

        // Check for inconsistent entries for this specific year/month
        foreach ($this->processedPdfs as $id => $info) {
            $filename = $info['filename'];
            
            // Check if this is a business PDF for the target year/month
            // Business PDFs have format: {orgId}{typeId}{yearMonth}.pdf
            if (preg_match('/' . preg_quote($yearMonth) . '\.pdf$/', $filename)) {
                $txtFile = "{$dataDir}/{$id}.txt";
                if (!file_exists($txtFile)) {
                    $toRemove[] = $id;
                    $this->logger->info("Found inconsistent business entry: {$filename} (missing {$id}.txt)");
                }
            }
        }

        // Remove inconsistent entries
        foreach ($toRemove as $id) {
            unset($this->processedPdfs[$id]);
            $cleaned++;
        }

        // Save updated tracking file if we cleaned anything
        if ($cleaned > 0) {
            $this->saveProcessedPdfs();
        }

        return $cleaned;
    }
}
