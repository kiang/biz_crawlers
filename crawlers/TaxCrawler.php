<?php

namespace BizData\Crawlers;

use Symfony\Component\DomCrawler\Crawler;

class TaxCrawler extends BaseCrawler
{
    private const TAX_DATA_URL = 'https://eip.fia.gov.tw/data/BGMOPEN1.zip';
    
    public function crawl(array $params = []): array
    {
        $this->logger->info("Starting tax data crawl");
        
        $csvFile = $params['file'] ?? null;
        
        if ($csvFile && file_exists($csvFile)) {
            $this->logger->info("Using provided CSV file: {$csvFile}");
            return $this->processCSVFile($csvFile);
        } else {
            $this->logger->info("Downloading latest tax data from FIA");
            return $this->downloadAndProcessTaxData();
        }
    }
    
    private function downloadAndProcessTaxData(): array
    {
        $zipFile = 'bgmopen1.zip';
        $csvFile = 'BGMOPEN1.csv';
        $oldCsvFile = 'bgmopen1.csv';
        
        try {
            // Download the ZIP file
            $this->logger->info("Downloading tax data ZIP file");
            $this->downloadFile(self::TAX_DATA_URL, $zipFile);
            
            // Extract the ZIP file
            $zip = new \ZipArchive();
            if ($zip->open($zipFile) === TRUE) {
                $zip->extractTo('.');
                $zip->close();
                $this->logger->info("Successfully extracted ZIP file");
            } else {
                throw new \Exception("Failed to extract ZIP file");
            }
            
            // Check if file has changed
            $hasChanged = true;
            if (file_exists($oldCsvFile)) {
                $oldMd5 = md5_file($oldCsvFile);
                $newMd5 = md5_file($csvFile);
                
                if ($oldMd5 === $newMd5) {
                    $this->logger->info("Tax data file has not changed");
                    $hasChanged = false;
                }
            }
            
            // Copy new file over old one
            if (file_exists($csvFile)) {
                copy($csvFile, $oldCsvFile);
                unlink($csvFile);
            }
            
            // Clean up ZIP file
            if (file_exists($zipFile)) {
                unlink($zipFile);
            }
            
            if (!$hasChanged) {
                return ['status' => 'no_changes', 'message' => 'Tax data file has not changed'];
            }
            
            return $this->processCSVFile($oldCsvFile);
            
        } catch (\Exception $e) {
            $this->logger->error("Failed to download and process tax data: " . $e->getMessage());
            throw $e;
        }
    }
    
    private function processCSVFile(string $csvFile): array
    {
        $this->logger->info("Processing CSV file: {$csvFile}");
        
        $handle = fopen($csvFile, 'r');
        if (!$handle) {
            throw new \Exception("Failed to open CSV file: {$csvFile}");
        }
        
        $processedData = [];
        $lineNumber = 0;
        $headers = null;
        
        try {
            while (($row = fgetcsv($handle, 0, ',')) !== FALSE) {
                $lineNumber++;
                
                // Skip comment lines
                if (strpos($row[0], '備註:') === 0 || strpos($row[0], '檔案產生日期：') === 0) {
                    continue;
                }
                
                // Clean up the row data
                $row = array_map(function($cell) {
                    return str_replace('　', '', trim($cell)); // Remove full-width spaces
                }, $row);
                
                // Process header row
                if (!$headers) {
                    $expectedHeaders = [
                        '營業地址', '統一編號', '總機構統一編號', '營業人名稱', '資本額', '設立日期',
                        '組織別名稱', '使用統一發票', '行業代號', '名稱', '行業代號1', '名稱1',
                        '行業代號2', '名稱2', '行業代號3', '名稱3'
                    ];
                    
                    if (implode(',', $row) !== implode(',', $expectedHeaders)) {
                        $this->logger->warning("CSV headers don't match expected format at line {$lineNumber}");
                        $this->logger->debug("Expected: " . implode(',', $expectedHeaders));
                        $this->logger->debug("Found: " . implode(',', $row));
                    }
                    
                    $headers = $row;
                    continue;
                }
                
                // Process data rows
                if (strlen($row[1]) !== 8) {
                    continue; // Skip rows without valid 8-digit tax ID
                }
                
                $taxData = $this->parseCSVRow($row, $headers);
                if ($taxData) {
                    $processedData[] = $taxData;
                }
                
                // Process in batches to avoid memory issues
                if (count($processedData) >= 10000) {
                    $this->logger->info("Processed {$lineNumber} lines, yielding batch of " . count($processedData) . " records");
                    yield $processedData;
                    $processedData = [];
                }
            }
            
            // Yield remaining data
            if (!empty($processedData)) {
                $this->logger->info("Final batch: " . count($processedData) . " records");
                yield $processedData;
            }
            
        } finally {
            fclose($handle);
        }
        
        $this->logger->info("Finished processing CSV file. Total lines processed: {$lineNumber}");
        
        return ['status' => 'completed', 'lines_processed' => $lineNumber];
    }
    
    private function parseCSVRow(array $row, array $headers): ?array
    {
        try {
            $splitColumnIndex = array_search('行業代號', $headers);
            if ($splitColumnIndex === false) {
                throw new \Exception("Cannot find '行業代號' column");
            }
            
            // Split the row into basic data and industry data
            $basicData = array_slice($row, 0, $splitColumnIndex);
            $industryData = array_slice($row, $splitColumnIndex);
            
            // Combine basic headers with basic data
            $basicHeaders = array_slice($headers, 0, $splitColumnIndex);
            $values = array_combine($basicHeaders, $basicData);
            
            // Process industry data in pairs
            $industries = [];
            for ($i = 0; $i < count($industryData); $i += 2) {
                if (isset($industryData[$i]) && isset($industryData[$i + 1])) {
                    $industryCode = trim($industryData[$i]);
                    $industryName = trim($industryData[$i + 1]);
                    
                    if ($industryCode && $industryName) {
                        $industries[] = [$industryCode, $industryName];
                    }
                }
            }
            
            $values['行業'] = $industries;
            
            // Parse establishment date (from ROC format to Western format)
            if (isset($values['設立日期']) && strlen($values['設立日期']) >= 7) {
                $dateStr = $values['設立日期'];
                $values['設立日期'] = [
                    'year' => intval(1911 + substr($dateStr, 0, 3)),
                    'month' => intval(substr($dateStr, 3, 2)),
                    'day' => intval(substr($dateStr, 5, 2))
                ];
            }
            
            // Store the tax ID separately for indexing
            $taxId = $values['統一編號'];
            unset($values['統一編號']);
            
            return [
                'tax_id' => $taxId,
                'data' => $values
            ];
            
        } catch (\Exception $e) {
            $this->logger->warning("Failed to parse CSV row: " . $e->getMessage());
            $this->logger->debug("Row data: " . json_encode($row));
            return null;
        }
    }
    
    public function validateTaxId(string $taxId): bool
    {
        // Basic validation for Taiwan tax ID (8 digits)
        return preg_match('/^\d{8}$/', $taxId) === 1;
    }
    
    public function normalizeTaxId(string $taxId): string
    {
        // Ensure tax ID is 8 digits with leading zeros if needed
        return sprintf('%08s', $taxId);
    }
}