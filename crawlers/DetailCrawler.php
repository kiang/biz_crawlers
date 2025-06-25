<?php

namespace BizData\Crawlers;

use Symfony\Component\DomCrawler\Crawler;

class DetailCrawler extends BaseCrawler
{
    private const BASE_URL = 'https://findbiz.nat.gov.tw/fts/query/QueryBar/queryInit.do';
    private const QUERY_URL = 'https://findbiz.nat.gov.tw/fts/query/QueryList/queryList.do';
    
    private static $lastFetch = null;
    private $sessionCurl = null;
    
    protected function getDefaultConfig(): array
    {
        $config = parent::getDefaultConfig();
        $config['rate_limit'] = 2; // seconds between requests (2 seconds to comply with server limit)
        $config['retry_delay'] = 3; // seconds to wait on failure (reduced)
        $config['max_retries'] = 1; // reduced to minimum
        $config['session_init_delay'] = 1; // minimal delay
        $config['search_delay'] = 2; // 2 seconds search delay to prevent rate limiting
        $config['fast_mode'] = false; // disable fast mode to ensure delays are respected
        return $config;
    }
    
    private function initializeSession(): void
    {
        if ($this->sessionCurl) {
            return;
        }
        
        $this->logger->info("Initializing session with findbiz.nat.gov.tw");
        
        // Initialize CURL with session support
        $this->sessionCurl = curl_init();
        curl_setopt($this->sessionCurl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($this->sessionCurl, CURLOPT_FRESH_CONNECT, true);
        curl_setopt($this->sessionCurl, CURLOPT_FORBID_REUSE, true);
        curl_setopt($this->sessionCurl, CURLOPT_COOKIEFILE, '');
        curl_setopt($this->sessionCurl, CURLOPT_USERAGENT, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:84.0) Gecko/20100101 Firefox/84.0');
        curl_setopt($this->sessionCurl, CURLOPT_DNS_USE_GLOBAL_CACHE, false);
        curl_setopt($this->sessionCurl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
        curl_setopt($this->sessionCurl, CURLOPT_TIMEOUT, 30);
        
        if ($this->config['proxy']) {
            curl_setopt($this->sessionCurl, CURLOPT_PROXY, $this->config['proxy']);
        }
        
        // First request to get session
        curl_setopt($this->sessionCurl, CURLOPT_URL, self::BASE_URL);
        curl_exec($this->sessionCurl);
        
        if (!$this->config['fast_mode']) {
            sleep($this->config['session_init_delay']);
        } else {
            // Minimal delay even in fast mode for session stability
            usleep(200000); // 0.2 seconds
        }
        
        // Second request to validation page
        curl_setopt($this->sessionCurl, CURLOPT_URL, self::QUERY_URL);
        curl_setopt($this->sessionCurl, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($this->sessionCurl, CURLOPT_HEADER, true);
        curl_setopt($this->sessionCurl, CURLINFO_HEADER_OUT, true);
        curl_exec($this->sessionCurl);
        
        $this->logger->info("Session initialized successfully");
    }
    
    private function rateLimitedFetch(): void
    {
        if (!$this->config['fast_mode'] && self::$lastFetch !== null) {
            $elapsed = microtime(true) - self::$lastFetch;
            $waitTime = $this->config['rate_limit'] - $elapsed;
            if ($waitTime > 0) {
                usleep($waitTime * 1000000);
            }
        }
        self::$lastFetch = microtime(true);
    }
    
    public function fetchCompanyDetail(string $id): ?array
    {
        // Check if detail was crawled within last 24 hours
        if ($this->isRecentlyCrawled($id, 'company')) {
            $this->logger->info("Skipping company {$id} - already crawled within 24 hours");
            return null;
        }
        
        $this->initializeSession();
        
        for ($retry = 0; $retry <= $this->config['max_retries']; $retry++) {
            try {
                // Always wait at least 2 seconds before making search request  
                $this->rateLimitedFetch();
                
                $this->logger->info("Fetching company detail for ID: {$id} (attempt " . ($retry + 1) . ")");
                
                // Search for company
                curl_setopt($this->sessionCurl, CURLOPT_URL, self::QUERY_URL);
                curl_setopt($this->sessionCurl, CURLOPT_POST, true);
                curl_setopt($this->sessionCurl, CURLOPT_POSTFIELDS, 
                    "errorMsg=&validatorOpen=N&rlPermit=0&userResp=&curPage=0&fhl=zh_TW&qryCond={$id}&infoType=D&qryType=cmpyType&cmpyType=true&brCmpyType=&busmType=&factType=&lmtdType=&isAlive=all&busiItemMain=&busiItemSub="
                );
                curl_setopt($this->sessionCurl, CURLOPT_REFERER, self::QUERY_URL);
                curl_setopt($this->sessionCurl, CURLOPT_HEADER, false);
                
                $content = curl_exec($this->sessionCurl);
                $info = curl_getinfo($this->sessionCurl);
                
                if ($info['http_code'] !== 200) {
                    throw new \Exception("HTTP error: " . $info['http_code']);
                }
                
                // Apply mandatory 2-second search delay
                sleep($this->config['search_delay']); // Always apply search delay to prevent rate limiting
                
                // Check for rate limiting
                if (strpos($content, '本系統限制使用者間隔2秒鐘才能進行下一次查詢') !== false) {
                    $this->logger->info("Rate limit detected for company {$id}, waiting 2 seconds and retrying");
                    // Save rate limited response for analysis
                    $this->saveRawHtml($id, $content, 'company', '_rate_limited_' . ($retry + 1));
                    sleep(2);
                    continue; // Retry the same attempt
                }
                
                if (strpos($content, '很抱歉，我們無法找到符合條件的查詢結果。') !== false) {
                    $this->logger->warning("No company found for ID: {$id}");
                    // Save search results HTML even when NOT FOUND for analysis
                    $this->saveRawHtml($id, $content, 'company', '_search_not_found');
                    return null;
                }
                
                // Save search results HTML for analysis
                $this->saveRawHtml($id, $content, 'company', '_search_results');
                
                // Parse search results to get detail URL
                $detailUrl = $this->parseSearchResults($content, 'company');
                if (!$detailUrl) {
                    $this->logger->error("Could not find detail URL in search results for company {$id}");
                    throw new \Exception("Could not find detail URL in search results");
                }
                
                $this->logger->info("Found detail URL for company {$id}: {$detailUrl}");
                
                // Fetch detailed information
                $this->rateLimitedFetch();
                $detailContent = $this->fetchDetailPage($detailUrl);
                
                // Check if we got a valid detail page
                if (empty($detailContent) || strlen($detailContent) < 1000) {
                    throw new \Exception("Received empty or invalid detail page content");
                }
                
                // Save raw HTML for analysis
                $this->saveRawHtml($id, $detailContent, 'company', '_detail_page');
                
                // Parse company details
                $companyData = $this->parseCompanyDetail($detailContent);
                
                // Validate that we got meaningful data
                if (empty($companyData) || count($companyData) < 3) {
                    $this->logger->error("Failed to parse company details for {$id} - insufficient data extracted");
                    // HTML already saved above as '_detail_page'
                    throw new \Exception("Failed to parse company details - insufficient data extracted");
                }
                
                $companyData['source_url'] = $detailUrl;
                $companyData['crawled_at'] = date('c');
                
                $this->logger->info("Successfully fetched company detail for ID: {$id}");
                return $companyData;
                
            } catch (\Exception $e) {
                $this->logger->warning("Attempt " . ($retry + 1) . " failed for company {$id}: " . $e->getMessage());
                
                if ($retry < $this->config['max_retries']) {
                    $waitTime = $this->config['retry_delay'] + $retry; // Linear instead of exponential
                    $this->logger->info("Waiting {$waitTime} seconds before retry");
                    sleep($waitTime);
                    
                    // Only reinitialize session every other retry to save time
                    if ($retry % 2 === 0) {
                        $this->closeSession();
                        $this->initializeSession();
                    }
                } else {
                    $this->logger->error("Failed to fetch company {$id} after " . ($retry + 1) . " attempts");
                    return null;
                }
            }
        }
        
        return null;
    }
    
    public function fetchBusinessDetail(string $id): ?array
    {
        // Check if detail was crawled within last 24 hours
        if ($this->isRecentlyCrawled($id, 'business')) {
            $this->logger->info("Skipping business {$id} - already crawled within 24 hours");
            return null;
        }
        
        $this->initializeSession();
        
        for ($retry = 0; $retry <= $this->config['max_retries']; $retry++) {
            try {
                // Always wait at least 2 seconds before making search request
                $this->rateLimitedFetch();
                
                $this->logger->info("Fetching business detail for ID: {$id} (attempt " . ($retry + 1) . ")");
                
                // Search for business
                curl_setopt($this->sessionCurl, CURLOPT_URL, self::QUERY_URL);
                curl_setopt($this->sessionCurl, CURLOPT_POST, true);
                curl_setopt($this->sessionCurl, CURLOPT_POSTFIELDS, 
                    "errorMsg=&validatorOpen=N&rlPermit=0&userResp=&curPage=0&fhl=zh_TW&qryCond={$id}&infoType=D&cmpyType=&brCmpyType=&qryType=busmType&busmType=true&factType=&lmtdType=&isAlive=all&busiItemMain=&busiItemSub="
                );
                curl_setopt($this->sessionCurl, CURLOPT_REFERER, self::QUERY_URL);
                curl_setopt($this->sessionCurl, CURLOPT_HEADER, false);
                
                $content = curl_exec($this->sessionCurl);
                $info = curl_getinfo($this->sessionCurl);
                
                if ($info['http_code'] !== 200) {
                    throw new \Exception("HTTP error: " . $info['http_code']);
                }
                
                // Apply mandatory 2-second search delay
                sleep($this->config['search_delay']); // Always apply search delay to prevent rate limiting
                
                // Check for rate limiting
                if (strpos($content, '本系統限制使用者間隔2秒鐘才能進行下一次查詢') !== false) {
                    $this->logger->info("Rate limit detected for business {$id}, waiting 2 seconds and retrying");
                    // Save rate limited response for analysis
                    $this->saveRawHtml($id, $content, 'business', '_rate_limited_' . ($retry + 1));
                    sleep(2);
                    continue; // Retry the same attempt
                }
                
                if (strpos($content, '很抱歉，我們無法找到符合條件的查詢結果。') !== false) {
                    $this->logger->warning("No business found for ID: {$id}");
                    // Save search results HTML even when NOT FOUND for analysis
                    $this->saveRawHtml($id, $content, 'business', '_search_not_found');
                    return null;
                }
                
                // Save search results HTML for analysis
                $this->saveRawHtml($id, $content, 'business', '_search_results');
                
                // Parse search results to get detail URL
                $detailUrl = $this->parseSearchResults($content, 'business');
                if (!$detailUrl) {
                    $this->logger->error("Could not find detail URL in search results for business {$id}");
                    throw new \Exception("Could not find detail URL in search results");
                }
                
                $this->logger->info("Found detail URL for business {$id}: {$detailUrl}");
                
                // Fetch detailed information
                $this->rateLimitedFetch();
                $detailContent = $this->fetchDetailPage($detailUrl);
                
                // Check if we got a valid detail page
                if (empty($detailContent) || strlen($detailContent) < 1000) {
                    throw new \Exception("Received empty or invalid detail page content");
                }
                
                // Save raw HTML for analysis
                $this->saveRawHtml($id, $detailContent, 'business', '_detail_page');
                
                // Parse business details
                $businessData = $this->parseBusinessDetail($detailContent);
                
                // Validate that we got meaningful data
                if (empty($businessData) || count($businessData) < 3) {
                    $this->logger->error("Failed to parse business details for {$id} - insufficient data extracted");
                    // HTML already saved above as '_detail_page'
                    throw new \Exception("Failed to parse business details - insufficient data extracted");
                }
                
                $businessData['source_url'] = $detailUrl;
                $businessData['crawled_at'] = date('c');
                
                $this->logger->info("Successfully fetched business detail for ID: {$id}");
                return $businessData;
                
            } catch (\Exception $e) {
                $this->logger->warning("Attempt " . ($retry + 1) . " failed for business {$id}: " . $e->getMessage());
                
                if ($retry < $this->config['max_retries']) {
                    $waitTime = $this->config['retry_delay'] + $retry; // Linear instead of exponential
                    $this->logger->info("Waiting {$waitTime} seconds before retry");
                    sleep($waitTime);
                    
                    // Only reinitialize session every other retry to save time
                    if ($retry % 2 === 0) {
                        $this->closeSession();
                        $this->initializeSession();
                    }
                } else {
                    $this->logger->error("Failed to fetch business {$id} after " . ($retry + 1) . " attempts");
                    return null;
                }
            }
        }
        
        return null;
    }
    
    private function parseSearchResults(string $content, string $type): ?string
    {
        // Handle UTF-8 encoding properly for Chinese characters
        $content = str_replace('<head>', '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">', $content);
        
        $doc = new \DOMDocument();
        @$doc->loadHTML($content);
        
        $table = $doc->getElementById('eslist-table');
        if (!$table) {
            return null;
        }
        
        $hitUrls = [];
        $tbody = $table->getElementsByTagName('tbody')->item(0);
        if (!$tbody) {
            return null;
        }
        
        foreach ($tbody->getElementsByTagName('tr') as $tr) {
            $tds = $tr->getElementsByTagName('td');
            if ($tds->length < 7) {
                continue;
            }
            
            $a = $tr->getElementsByTagName('a')->item(0);
            if (!$a) {
                continue;
            }
            
            $href = preg_replace('#\s*#', '', $a->getAttribute('href'));
            $expectedPath = $type === 'company' ? '/fts/query/QueryCmpyDetail/queryCmpyDetail.do' : '/fts/query/QueryBusmDetail/queryBusmDetail.do';
            
            if (strpos($href, $expectedPath) === false) {
                continue;
            }
            
            // Get the change date for sorting
            $date = $tds->item(6)->nodeValue;
            if ($tds->item(6)->getAttribute('data-title') !== '核准變更日期') {
                continue;
            }
            
            $hitUrls[$href] = $date;
        }
        
        if (empty($hitUrls)) {
            return null;
        }
        
        // Sort by date descending and return the most recent
        arsort($hitUrls);
        $urls = array_keys($hitUrls);
        return 'https://findbiz.nat.gov.tw' . $urls[0];
    }
    
    private function fetchDetailPage(string $url): string
    {
        curl_setopt($this->sessionCurl, CURLOPT_URL, $url);
        curl_setopt($this->sessionCurl, CURLOPT_POST, false);
        curl_setopt($this->sessionCurl, CURLOPT_REFERER, self::QUERY_URL);
        
        $content = curl_exec($this->sessionCurl);
        $info = curl_getinfo($this->sessionCurl);
        
        if ($info['http_code'] !== 200) {
            throw new \Exception("Failed to fetch detail page: HTTP " . $info['http_code']);
        }
        
        return $content;
    }
    
    private function parseCompanyDetail(string $content): array
    {
        // Debug: Check if expected content is present
        if (strpos($content, 'tabCmpyContent') === false) {
            $this->logger->warning("HTML does not contain tabCmpyContent element");
        }
        
        // Handle UTF-8 encoding properly for Chinese characters
        // Use meta tag to specify UTF-8 encoding for DOMDocument  
        $content = preg_replace('/<head[^>]*>/i', '<head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8">', $content);
        
        $doc = new \DOMDocument();
        @$doc->loadHTML($content);
        
        $data = [];
        
        // Parse basic company information
        $tabDiv = $doc->getElementById('tabCmpyContent');
        $this->logger->debug("Looking for tabCmpyContent div: " . ($tabDiv ? 'FOUND' : 'NOT FOUND'));
        
        if ($tabDiv) {
            // Find table within the div
            $tables = $tabDiv->getElementsByTagName('table');
            $table = $tables->length > 0 ? $tables->item(0) : null;
            $this->logger->debug("Looking for table within div: " . ($table ? 'FOUND' : 'NOT FOUND'));
            
            if ($table) {
                $tbody = $table->getElementsByTagName('tbody')->item(0);
                $this->logger->debug("Looking for tbody: " . ($tbody ? 'FOUND' : 'NOT FOUND'));
                if ($tbody) {
                foreach ($tbody->childNodes as $tr) {
                    if ($tr->nodeName !== 'tr') {
                        continue;
                    }
                    
                    $tds = $tr->getElementsByTagName('td');
                    if ($tds->length < 2) {
                        continue;
                    }
                    
                    $key = trim($tds->item(0)->nodeValue);
                    $value = trim($tds->item(1)->nodeValue);
                    
                    // Clean specific fields following the working pattern from Updater2.php
                    if ($key === '統一編號') {
                        $value = str_replace(html_entity_decode('&nbsp;'), '', $value);
                    } elseif ($key === '公司名稱') {
                        // Remove unwanted text patterns for company name
                        $value = preg_replace('/本項查詢服務.*?關閉/s', '', $value);
                        $value = trim($value);
                    } elseif ($key === '公司所在地') {
                        // Remove "電子地圖" from address
                        $value = str_replace('電子地圖', '', $value);
                        $value = trim($value);
                    } elseif ($key === '登記現況') {
                        // Remove the tax query message
                        $value = preg_replace('/「查詢最新營業狀況請至.*?」/', '', $value);
                        $value = str_replace(html_entity_decode('&nbsp;'), '', $value);
                        $value = trim($value);
                    }
                    
                    // Parse dates
                    if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                        $data[$key] = [
                            'year' => intval($matches[1]) + 1911,
                            'month' => intval($matches[2]),
                            'day' => intval($matches[3]),
                            'formatted' => $value
                        ];
                    } elseif ($key === '所營事業資料') {
                        // Parse business data as structured array similar to Updater2.php
                        $businessItems = [];
                        $lines = explode("\n", trim($value));
                        foreach ($lines as $line) {
                            $line = trim($line);
                            if (empty($line)) continue;
                            if (preg_match('/^([A-Z]\d{6})\s+(.+)$/', $line, $matches)) {
                                $businessItems[] = [
                                    'code' => $matches[1],
                                    'description' => trim($matches[2])
                                ];
                            }
                        }
                        // If no structured data found, store as raw data
                        $data[$key] = !empty($businessItems) ? $businessItems : ['raw_data' => $value];
                    } else {
                        $data[$key] = $value;
                    }
                }
            }
        }
        }
        
        $this->logger->debug("Parsed company data fields: " . count($data));
        
        // Parse shareholders
        $shareDiv = $doc->getElementById('tabShareHolderContent');
        if ($shareDiv) {
            $shareholders = [];
            $tables = $shareDiv->getElementsByTagName('table');
            $table = $tables->length > 0 ? $tables->item(0) : null;
            if ($table) {
                $tbody = $table->getElementsByTagName('tbody')->item(0);
                if ($tbody) {
                foreach ($tbody->childNodes as $tr) {
                    if ($tr->nodeName !== 'tr') {
                        continue;
                    }
                    
                    $tds = $tr->getElementsByTagName('td');
                    if ($tds->length !== 5) {
                        continue;
                    }
                    
                    $shareholder = [
                        'sequence' => trim($tds->item(0)->nodeValue),
                        'position' => trim($tds->item(1)->nodeValue),
                        'name' => trim($tds->item(2)->nodeValue),
                        'representative' => trim($tds->item(3)->nodeValue),
                        'investment' => trim($tds->item(4)->nodeValue)
                    ];
                    
                    $shareholders[] = $shareholder;
                }
            }
            }
            $data['shareholders'] = $shareholders;
        }
        
        // Parse managers
        $mgrDiv = $doc->getElementById('tabMgrContent');
        if ($mgrDiv) {
            $managers = [];
            $tables = $mgrDiv->getElementsByTagName('table');
            $table = $tables->length > 0 ? $tables->item(0) : null;
            if ($table) {
                $tbody = $table->getElementsByTagName('tbody')->item(0);
                if ($tbody) {
                foreach ($tbody->childNodes as $tr) {
                    if ($tr->nodeName !== 'tr') {
                        continue;
                    }
                    
                    $tds = $tr->getElementsByTagName('td');
                    if ($tds->length !== 3) {
                        continue;
                    }
                    
                    $appointDate = trim($tds->item(2)->nodeValue);
                    $parsedDate = null;
                    if (preg_match('/(\d+)年(\d+)月(\d+)日/', $appointDate, $matches)) {
                        $parsedDate = [
                            'year' => 1911 + intval($matches[1]),
                            'month' => intval($matches[2]),
                            'day' => intval($matches[3])
                        ];
                    }
                    
                    $manager = [
                        'sequence' => trim($tds->item(0)->nodeValue),
                        'name' => trim($tds->item(1)->nodeValue),
                        'appoint_date' => $parsedDate
                    ];
                    
                    $managers[] = $manager;
                }
            }
            }
            $data['managers'] = $managers;
        }
        
        // Log final parsing results
        $this->logger->debug("Final parsed company data contains " . count($data) . " fields");
        if (empty($data)) {
            $this->logger->error("No company data was parsed from the HTML content");
        }
        
        return $data;
    }
    
    private function parseBusinessDetail(string $content): array
    {
        // Handle UTF-8 encoding properly for Chinese characters  
        $content = str_replace('<head>', '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">', $content);
        
        $doc = new \DOMDocument();
        @$doc->loadHTML($content);
        
        $data = [];
        
        // Parse basic business information
        if ($table = $doc->getElementById('tabBusmContent')) {
            $tbody = $table->getElementsByTagName('tbody')->item(0);
            if ($tbody) {
                foreach ($tbody->childNodes as $tr) {
                    if ($tr->nodeName !== 'tr') {
                        continue;
                    }
                    
                    $tds = $tr->getElementsByTagName('td');
                    if ($tds->length < 2) {
                        continue;
                    }
                    
                    $key = trim($tds->item(0)->nodeValue);
                    $value = trim($tds->item(1)->nodeValue);
                    
                    // Parse dates
                    if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                        $data[$key] = [
                            'year' => intval($matches[1]) + 1911,
                            'month' => intval($matches[2]),
                            'day' => intval($matches[3]),
                            'formatted' => $value
                        ];
                    } else {
                        $data[$key] = $value;
                    }
                }
            }
        }
        
        return $data;
    }
    
    public function saveCompanyDetail(string $id, array $data): string
    {
        $this->logger->debug("saveCompanyDetail called for {$id} with " . count($data) . " fields");
        if (empty($data)) {
            $this->logger->error("saveCompanyDetail received empty data for {$id}");
        }
        
        $dataDir = dirname(__DIR__) . '/data/gcis/companies/details';
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }
        
        $filename = "{$id}.json";
        $filepath = "{$dataDir}/{$filename}";
        
        // Clean data recursively to ensure JSON encoding works
        $cleanData = $this->cleanDataForJson($data);
        
        $saveData = [
            'metadata' => [
                'id' => $id,
                'type' => 'company_detail',
                'crawled_at' => date('c'),
                'source' => 'findbiz.nat.gov.tw'
            ],
            'data' => $cleanData
        ];
        
        // Try encoding with different options to handle UTF-8 issues
        $jsonData = json_encode($saveData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
        
        if ($jsonData === false) {
            $this->logger->warning("JSON encoding failed with UNESCAPED_UNICODE: " . json_last_error_msg());
            // Try without JSON_UNESCAPED_UNICODE
            $jsonData = json_encode($saveData, JSON_PRETTY_PRINT);
            
            if ($jsonData === false) {
                $this->logger->warning("JSON encoding failed without UNESCAPED_UNICODE: " . json_last_error_msg());
                // Last resort: try with escaped unicode and no pretty print
                $jsonData = json_encode($saveData);
                
                if ($jsonData === false) {
                    $this->logger->error("All JSON encoding attempts failed for {$id}: " . json_last_error_msg());
                    // Save error information instead of empty file
                    $errorData = [
                        'error' => 'JSON encoding failed',
                        'message' => json_last_error_msg(),
                        'id' => $id,
                        'timestamp' => date('c'),
                        'field_count' => count($data)
                    ];
                    $jsonData = json_encode($errorData);
                }
            }
        }
        
        file_put_contents($filepath, $jsonData);
        
        $this->logger->info("Saved company detail for {$id} to {$filepath} (" . strlen($jsonData) . " bytes)");
        return $filepath;
    }
    
    public function saveBusinessDetail(string $id, array $data): string
    {
        $dataDir = dirname(__DIR__) . '/data/gcis/businesses/details';
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }
        
        $filename = "{$id}.json";
        $filepath = "{$dataDir}/{$filename}";
        
        $saveData = [
            'metadata' => [
                'id' => $id,
                'type' => 'business_detail',
                'crawled_at' => date('c'),
                'source' => 'findbiz.nat.gov.tw'
            ],
            'data' => $data
        ];
        
        file_put_contents($filepath, json_encode($saveData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
        
        $this->logger->info("Saved business detail for {$id} to {$filepath}");
        return $filepath;
    }
    
    private function cleanFieldValue(string $value): string
    {
        // Remove excessive whitespace, tabs, and carriage returns
        $value = preg_replace('/[\r\n\t]+/', ' ', $value);
        
        // Remove common unwanted text patterns
        $unwantedPatterns = [
            '/\s*訂閱\s*$/',
            '/\s*Google搜尋\s*/',
            '/\s*電子地圖\s*/',
            '/\s*地址所屬公司家數:\s*\d+\s*/',
            '/「查詢最新營業狀況請至.*?」/',
            '/「國際貿易署廠商英文名稱查詢.*?」/',
            '/「國際貿易署廠商英文名稱查詢」本項查詢服務.*?關閉/s',
            '/本項查詢服務.*?關閉/s',
            '/客服專線：.*?$/',
            '/已了解，開始查詢.*?$/',
            '/\s*關閉\s*$/',
        ];
        
        foreach ($unwantedPatterns as $pattern) {
            $value = preg_replace($pattern, '', $value);
        }
        
        // Clean up multiple spaces and trim
        $value = preg_replace('/\s+/', ' ', $value);
        $value = trim($value);
        
        return $value;
    }
    
    private function parseBusinessItems(string $businessData): array
    {
        $items = [];
        
        if (empty(trim($businessData))) {
            return $items;
        }
        
        // Split by business codes (pattern: letter followed by numbers and space)
        $parts = preg_split('/([A-Z]\d{6})\s+/', $businessData, -1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);
        
        for ($i = 0; $i < count($parts); $i += 2) {
            if (isset($parts[$i]) && isset($parts[$i + 1])) {
                $code = trim($parts[$i]);
                $description = trim($parts[$i + 1]);
                
                if (!empty($code) && !empty($description)) {
                    $items[] = [
                        'code' => $code,
                        'description' => $description
                    ];
                }
            }
        }
        
        // If no items parsed, return the raw data for debugging
        if (empty($items) && !empty(trim($businessData))) {
            return ['raw_data' => trim($businessData)];
        }
        
        return $items;
    }
    
    private function trimKeyField(string $value): string
    {
        // Remove any remaining unwanted characters that might slip through
        $value = preg_replace('/\s+/', ' ', $value); // Normalize spaces
        $value = trim($value);
        
        return $value;
    }
    
    private function cleanDataForJson($data)
    {
        if (is_array($data)) {
            $result = [];
            foreach ($data as $key => $value) {
                $cleanKey = $this->cleanStringForJson($key);
                $result[$cleanKey] = $this->cleanDataForJson($value);
            }
            return $result;
        } elseif (is_string($data)) {
            return $this->cleanStringForJson($data);
        } else {
            return $data;
        }
    }
    
    private function cleanStringForJson(string $value): string
    {
        // Only remove control characters that actually break JSON encoding
        // Avoid any encoding conversion that might corrupt Chinese characters
        $value = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/', '', $value);
        
        return trim($value);
    }
    
    private function parseCompanyDataWithRegex(string $content): array
    {
        $data = [];
        
        // Define field mappings from the table
        $fields = [
            '統一編號' => '',
            '登記現況' => '',
            '公司名稱' => '',
            '章程所訂外文公司名稱' => '',
            '資本總額(元)' => '',
            '代表人姓名' => '',
            '公司所在地' => '',
            '登記機關' => '',
            '核准設立日期' => '',
            '最後核准變更日期' => '',
            '所營事業資料' => ''
        ];
        
        // Extract the tabCmpyContent section
        if (preg_match('/<div[^>]*id="tabCmpyContent"[^>]*>(.*?)<\/div>/s', $content, $tabMatch)) {
            $tabContent = $tabMatch[1];
            
            // Extract each table row
            if (preg_match_all('/<tr[^>]*>(.*?)<\/tr>/s', $tabContent, $rowMatches)) {
                foreach ($rowMatches[1] as $rowContent) {
                    // Extract cells from each row
                    if (preg_match_all('/<td[^>]*class="txt_td"[^>]*>(.*?)<\/td>\s*<td[^>]*>(.*?)<\/td>/s', $rowContent, $cellMatches)) {
                        for ($i = 0; $i < count($cellMatches[1]); $i++) {
                            $key = strip_tags($cellMatches[1][$i]);
                            $value = strip_tags($cellMatches[2][$i]);
                            
                            // Clean the field value
                            $key = trim($key);
                            $value = $this->cleanFieldValue($value);
                            
                            if (!empty($key) && array_key_exists($key, $fields)) {
                                // Apply additional trimming for key fields
                                if (in_array($key, ['統一編號', '登記現況', '公司名稱', '公司所在地'])) {
                                    $value = $this->trimKeyField($value);
                                }
                                
                                // Parse dates
                                if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                                    $data[$key] = [
                                        'year' => intval($matches[1]) + 1911,
                                        'month' => intval($matches[2]),
                                        'day' => intval($matches[3]),
                                        'formatted' => $value
                                    ];
                                } elseif ($key === '所營事業資料') {
                                    $data[$key] = $this->parseBusinessItems($value);
                                } else {
                                    $data[$key] = $value;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        $this->logger->debug("Regex parsed company data fields: " . count($data));
        
        // Parse shareholders using regex (simplified version)
        $data['shareholders'] = [];
        
        // Parse managers using regex (simplified version)  
        $data['managers'] = [];
        
        // Log final parsing results
        $this->logger->debug("Final regex parsed company data contains " . count($data) . " fields");
        if (empty($data)) {
            $this->logger->error("No company data was parsed from the HTML content using regex");
        }
        
        return $data;
    }
    
    private function saveRawHtml(string $id, string $content, string $type, string $suffix = ''): string
    {
        $dataDir = dirname(__DIR__) . "/data/raw/{$type}s";
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }
        
        $filename = "{$id}{$suffix}.html";
        $filepath = "{$dataDir}/{$filename}";
        
        file_put_contents($filepath, $content);
        
        $this->logger->debug("Saved raw HTML for {$type} {$id} to {$filepath}");
        return $filepath;
    }
    
    private function isRecentlyCrawled(string $id, string $type): bool
    {
        $dataDir = dirname(__DIR__) . "/data/gcis/{$type}s/details";
        $filepath = "{$dataDir}/{$id}.json";
        
        if (!file_exists($filepath)) {
            return false;
        }
        
        $fileModTime = filemtime($filepath);
        $currentTime = time();
        $hoursDiff = ($currentTime - $fileModTime) / 3600;
        
        return $hoursDiff < 24;
    }
    
    private function closeSession(): void
    {
        if ($this->sessionCurl) {
            curl_close($this->sessionCurl);
            $this->sessionCurl = null;
        }
    }
    
    public function __destruct()
    {
        $this->closeSession();
    }
    
    public function crawl(array $params = []): array
    {
        // This method is required by the abstract parent class
        // For detail crawler, we'll implement batch processing here
        $ids = $params['ids'] ?? [];
        $type = $params['type'] ?? 'company'; // 'company' or 'business'
        
        $results = [];
        
        foreach ($ids as $id) {
            try {
                if ($type === 'company') {
                    $data = $this->fetchCompanyDetail($id);
                    if ($data) {
                        $this->saveCompanyDetail($id, $data);
                        $results[] = $id;
                    }
                } else {
                    $data = $this->fetchBusinessDetail($id);
                    if ($data) {
                        $this->saveBusinessDetail($id, $data);
                        $results[] = $id;
                    }
                }
            } catch (\Exception $e) {
                $this->logger->error("Failed to process {$type} {$id}: " . $e->getMessage());
            }
        }
        
        return $results;
    }
}