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
        $config['rate_limit'] = 1; // seconds between requests (aggressive default)
        $config['retry_delay'] = 3; // seconds to wait on failure (reduced)
        $config['max_retries'] = 1; // reduced to minimum
        $config['session_init_delay'] = 1; // minimal delay
        $config['search_delay'] = 1; // minimal search delay
        $config['fast_mode'] = true; // fast mode by default
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
                
                // Apply search delay based on mode
                if ($this->config['fast_mode']) {
                    usleep(500000); // 0.5 seconds in fast mode
                } else {
                    sleep($this->config['search_delay']); // Standard delay
                }
                
                if (strpos($content, '很抱歉，我們無法找到符合條件的查詢結果。') !== false) {
                    $this->logger->warning("No company found for ID: {$id}");
                    return null;
                }
                
                // Parse search results to get detail URL
                $detailUrl = $this->parseSearchResults($content, 'company');
                if (!$detailUrl) {
                    throw new \Exception("Could not find detail URL in search results");
                }
                
                // Fetch detailed information
                $this->rateLimitedFetch();
                $detailContent = $this->fetchDetailPage($detailUrl);
                
                // Parse company details
                $companyData = $this->parseCompanyDetail($detailContent);
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
                
                // Apply search delay based on mode
                if ($this->config['fast_mode']) {
                    usleep(500000); // 0.5 seconds in fast mode
                } else {
                    sleep($this->config['search_delay']); // Standard delay
                }
                
                if (strpos($content, '很抱歉，我們無法找到符合條件的查詢結果。') !== false) {
                    $this->logger->warning("No business found for ID: {$id}");
                    return null;
                }
                
                // Parse search results to get detail URL
                $detailUrl = $this->parseSearchResults($content, 'business');
                if (!$detailUrl) {
                    throw new \Exception("Could not find detail URL in search results");
                }
                
                // Fetch detailed information
                $this->rateLimitedFetch();
                $detailContent = $this->fetchDetailPage($detailUrl);
                
                // Parse business details
                $businessData = $this->parseBusinessDetail($detailContent);
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
        $content = str_replace('<head>', '<head><meta http-equiv="Content-Type" content="text/html; charset=utf-8">', $content);
        $doc = new \DOMDocument();
        @$doc->loadHTML($content);
        
        $data = [];
        
        // Parse basic company information
        if ($table = $doc->getElementById('tabCmpyContent')) {
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
        
        // Parse shareholders
        if ($table = $doc->getElementById('tabShareHolderContent')) {
            $shareholders = [];
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
            $data['shareholders'] = $shareholders;
        }
        
        // Parse managers
        if ($table = $doc->getElementById('tabMgrContent')) {
            $managers = [];
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
            $data['managers'] = $managers;
        }
        
        return $data;
    }
    
    private function parseBusinessDetail(string $content): array
    {
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
        $dataDir = dirname(__DIR__) . '/data/gcis/companies/details';
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }
        
        $filename = "{$id}.json";
        $filepath = "{$dataDir}/{$filename}";
        
        $saveData = [
            'metadata' => [
                'id' => $id,
                'type' => 'company_detail',
                'crawled_at' => date('c'),
                'source' => 'findbiz.nat.gov.tw'
            ],
            'data' => $data
        ];
        
        file_put_contents($filepath, json_encode($saveData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
        
        $this->logger->info("Saved company detail for {$id} to {$filepath}");
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