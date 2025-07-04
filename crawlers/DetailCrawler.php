<?php

/**
 * DetailCrawler for Taiwan Business Registry (findbiz.nat.gov.tw)
 * 
 * Original crawler implementation by Ronny Wang (@ronnywang)
 * Enhanced and modified for this project
 */

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
        curl_setopt($this->sessionCurl, CURLOPT_USERAGENT, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        curl_setopt($this->sessionCurl, CURLOPT_HTTPHEADER, [
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language: zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding: gzip, deflate, br',
            'DNT: 1',
            'Connection: keep-alive',
            'Upgrade-Insecure-Requests: 1',
            'Sec-Fetch-Dest: document',
            'Sec-Fetch-Mode: navigate',
            'Sec-Fetch-Site: none',
            'Sec-Fetch-User: ?1',
            'sec-ch-ua: "Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile: ?0',
            'sec-ch-ua-platform: "Windows"'
        ]);
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
        // Check if we have existing raw HTML file to regenerate from
        $rawHtmlPath = $this->getRawHtmlPath($id, 'company', '_detail_page');
        if (file_exists($rawHtmlPath)) {
            $this->logger->info("Found existing raw HTML for company {$id}, regenerating JSON from raw file");
            $content = file_get_contents($rawHtmlPath);
            if (!empty($content)) {
                $companyData = $this->parseCompanyDetail($content);
                if (!empty($companyData) && count($companyData) >= 3) {
                    $companyData['crawled_at'] = date('c');

                    $this->logger->info("Successfully regenerated company detail for ID: {$id} from raw HTML");
                    return $companyData;
                } else {
                    $this->logger->warning("Failed to parse existing raw HTML for company {$id}");
                }
            }
        }

        // Check if detail was crawled within last 24 hours (only if no raw HTML exists)
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
                curl_setopt(
                    $this->sessionCurl,
                    CURLOPT_POSTFIELDS,
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
        // Check if we have existing raw HTML file to regenerate from
        $rawHtmlPath = $this->getRawHtmlPath($id, 'business', '_detail_page');
        if (file_exists($rawHtmlPath)) {
            $this->logger->info("Found existing raw HTML for business {$id}, regenerating JSON from raw file");
            $content = file_get_contents($rawHtmlPath);
            if (!empty($content)) {
                $businessData = $this->parseBusinessDetail($content);
                if (!empty($businessData) && count($businessData) >= 3) {
                    $businessData['crawled_at'] = date('c');

                    $this->logger->info("Successfully regenerated business detail for ID: {$id} from raw HTML");
                    return $businessData;
                } else {
                    $this->logger->warning("Failed to parse existing raw HTML for business {$id}");
                }
            }
        }

        // Check if detail was crawled within last 24 hours (only if no raw HTML exists)
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
                curl_setopt(
                    $this->sessionCurl,
                    CURLOPT_POSTFIELDS,
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

                        switch ($key) {
                            case '統一編號':
                                // Skip 統一編號 as it's duplicated with id field
                                break;
                            case '公司名稱':
                            case '在中華民國境內負責人':
                            case '在中華民國境內代表人':
                                // Extract both Chinese and English company names
                                $rawHtml = $doc->saveHTML($tds->item(1));
                                $pos = strpos($rawHtml, '<span id="linkMoea">');
                                if (false !== $pos) {
                                    $rawHtml = substr($rawHtml, 0, $pos);
                                }
                                // Remove all span and div elements and their content completely first
                                $cleanHtml = strip_tags(preg_replace('/<span[^>]*>.*?<\/span>/is', '', $rawHtml));

                                // Split by <br> tags to get separate names
                                $parts = preg_split('/\n/i', $cleanHtml);

                                $theNames = [];
                                foreach ($parts as $part) {
                                    // Remove remaining HTML tags
                                    $cleanName = trim($part);

                                    // Only keep non-empty names that look like actual company names
                                    if (!empty($cleanName) && strlen($cleanName) > 2) {
                                        $theNames[] = $cleanName;
                                    }
                                }

                                // If we found names, use them; prefer multiple names over single name
                                if (!empty($theNames)) {
                                    if (!isset($data[$key]) || (is_string($data[$key]) && count($theNames) > 1)) {
                                        $data[$key] = count($theNames) > 1 ? $theNames : $theNames[0];
                                    }
                                }
                                break;
                            case '公司所在地':
                            case '地址':
                            case '辦事處所在地':
                            case '登記現況':
                            case '分公司所在地':
                            case '負責人姓名':
                                $pos = strpos($value, chr(13));
                                if (false !== $pos) {
                                    $value = trim(substr($value, 0, $pos));
                                } else {
                                    $value = trim($value);
                                }
                                $value = preg_replace('/\s/', '', $value);
                                $value = str_replace(html_entity_decode('&nbsp;'), '', $value);
                                $data[$key] = $value;
                                break;
                            case '所營事業資料':
                                // Parse business data by finding code positions and extracting sections
                                $businessItems = [];

                                // Find all business code positions in the text
                                if (preg_match_all('/([A-Z][A-Z0-9]\d{5})/', $value, $matches, PREG_OFFSET_CAPTURE)) {
                                    $codes = $matches[1];

                                    for ($i = 0; $i < count($codes); $i++) {
                                        $code = $codes[$i][0];
                                        $startPos = $codes[$i][1];

                                        // Find end position (next code or end of text)
                                        $endPos = isset($codes[$i + 1]) ? $codes[$i + 1][1] : strlen($value);

                                        // Extract the section from start to end
                                        $section = substr($value, $startPos, $endPos - $startPos);

                                        // Split by whitespace characters (space, newline, tab, etc.)
                                        $parts = preg_split('/\s+/', trim($section), -1, PREG_SPLIT_NO_EMPTY);

                                        // First part should be the code, rest is description
                                        if (count($parts) > 1) {
                                            array_shift($parts); // Remove the code part
                                            $description = implode(' ', $parts);

                                            $businessItems[] = [
                                                $code,
                                                trim($description)
                                            ];
                                        }
                                    }
                                }
                                // If no structured data found, split by line breaks and trim each line
                                if (!empty($businessItems)) {
                                    $data[$key] = $businessItems;
                                } else {
                                    $lines = array_filter(array_map('trim', explode("\n", $value)));
                                    $data[$key] = $lines;
                                }
                                break;
                            default:
                                if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                                    $data[$key] = [
                                        'year' => intval($matches[1]) + 1911,
                                        'month' => intval($matches[2]),
                                        'day' => intval($matches[3])
                                    ];
                                } else {
                                    $data[$key] = $value;
                                }
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
            // Skip first empty table, use second table which contains the actual data
            $table = $tables->length > 1 ? $tables->item(1) : ($tables->length > 0 ? $tables->item(0) : null);
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

                        // Parse 所代表法人 field to extract company ID and name if it's a link
                        $legalEntityHtml = $doc->saveHTML($tds->item(3));
                        $legalEntityValue = trim($tds->item(3)->nodeValue);

                        // Check if there's a link with queryCmpy function containing company ID
                        if (preg_match("/queryCmpy\('([^']+)','(\d+)',/", $legalEntityHtml, $matches)) {
                            $companyName = $matches[1];
                            $companyId = $matches[2];
                            $legalEntityValue = [$companyId, $companyName];
                        } elseif (!empty($legalEntityValue)) {
                            // Keep as string if not empty but no company ID found
                            $legalEntityValue = $legalEntityValue;
                        } else {
                            // Empty string for empty values
                            $legalEntityValue = '';
                        }

                        $shareholder = [
                            '序號' => trim($tds->item(0)->nodeValue),
                            '職稱' => trim($tds->item(1)->nodeValue),
                            '姓名' => trim($tds->item(2)->nodeValue),
                            '所代表法人' => $legalEntityValue,
                            '出資額' => trim($tds->item(4)->nodeValue)
                        ];

                        $shareholders[] = $shareholder;
                    }
                }
            }
            $data['董監事名單'] = $shareholders;
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
                            '序號' => trim($tds->item(0)->nodeValue),
                            '姓名' => trim($tds->item(1)->nodeValue),
                            '就任日期' => $parsedDate
                        ];

                        $managers[] = $manager;
                    }
                }
            }
            $data['經理人名單'] = $managers;
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

                    switch ($key) {
                        case '商業統一編號':
                            // Skip 統一編號 as it's duplicated with id field
                            break;
                        case '地址':
                        case '登記現況':
                            $pos = strpos($value, chr(13));
                            if (false !== $pos) {
                                $value = trim(substr($value, 0, $pos));
                            } else {
                                $value = trim($value);
                            }
                            $value = preg_replace('/\s/', '', $value);
                            $value = str_replace(html_entity_decode('&nbsp;'), '', $value);
                            $data[$key] = $value;
                            break;
                        case '商業名稱':
                            // Extract business name and clean unwanted content
                            $rawHtml = $doc->saveHTML($tds->item(1));
                            $pos = strpos($rawHtml, '<span id="linkMoea">');
                            if (false !== $pos) {
                                $rawHtml = substr($rawHtml, 0, $pos);
                            }
                            // Remove all span and div elements and their content completely first
                            $cleanHtml = strip_tags(preg_replace('/<span[^>]*>.*?<\/span>/is', '', $rawHtml));

                            // Split by <br> tags to get separate names
                            $parts = preg_split('/\n/i', $cleanHtml);

                            $theNames = [];
                            foreach ($parts as $part) {
                                // Remove remaining HTML tags
                                $cleanName = trim($part);

                                // Only keep non-empty names that look like actual names
                                if (!empty($cleanName) && strlen($cleanName) > 2) {
                                    $theNames[] = $cleanName;
                                }
                            }

                            // If we found names, use them; prefer multiple names over single name
                            if (!empty($theNames)) {
                                if (!isset($data[$key]) || (is_string($data[$key]) && count($theNames) > 1)) {
                                    $data[$key] = count($theNames) > 1 ? $theNames : $theNames[0];
                                }
                            }
                            break;
                        case '負責人姓名':
                            // Parse the nested table structure for responsible person name and investment
                            $cellHtml = $doc->saveHTML($tds->item(1));

                            // Look for table structure within the cell
                            if (preg_match('/<table[^>]*>(.*?)<\/table>/is', $cellHtml, $tableMatch)) {
                                // Parse the inner table to extract name and investment amount
                                $innerDoc = new \DOMDocument();
                                @$innerDoc->loadHTML('<html><head><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"></head><body><table>' . $tableMatch[1] . '</table></body></html>');

                                $innerTable = $innerDoc->getElementsByTagName('table')->item(0);
                                if ($innerTable) {
                                    $innerTbody = $innerTable->getElementsByTagName('tbody')->item(0);
                                    $innerTr = $innerTbody ? $innerTbody->getElementsByTagName('tr')->item(0) : $innerTable->getElementsByTagName('tr')->item(0);

                                    if ($innerTr) {
                                        $innerTds = $innerTr->getElementsByTagName('td');
                                        if ($innerTds->length >= 2) {
                                            // First column: person name
                                            $personName = trim($innerTds->item(0)->nodeValue);

                                            // Second column: investment amount info  
                                            $investmentInfo = trim($innerTds->item(1)->nodeValue);

                                            // Extract amount from "出資額(元):123456" format
                                            $amount = 0;
                                            if (preg_match('/出資額\(元\):(\d+)/', $investmentInfo, $matches)) {
                                                $amount = intval($matches[1]);
                                            }

                                            // Set the responsible person name
                                            if (!empty($personName)) {
                                                $data['負責人姓名'] = $personName;
                                            }

                                            // Create or append to investment amount array
                                            if (!isset($data['出資額(元)'])) {
                                                $data['出資額(元)'] = [];
                                            }
                                            if (!empty($personName)) {
                                                $data['出資額(元)'][] = [$personName => $amount];
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Fallback to simple text parsing if no table structure
                                $data[$key] = trim($tds->item(1)->nodeValue);
                            }
                            break;
                        case '營業項目':
                                // Parse business data by finding code positions and extracting sections
                                $businessItems = [];

                                // Find all business code positions in the text
                                if (preg_match_all('/([A-Z][A-Z0-9]\d{5})/', $value, $matches, PREG_OFFSET_CAPTURE)) {
                                    $codes = $matches[1];

                                    for ($i = 0; $i < count($codes); $i++) {
                                        $code = $codes[$i][0];
                                        $startPos = $codes[$i][1];

                                        // Find end position (next code or end of text)
                                        $endPos = isset($codes[$i + 1]) ? $codes[$i + 1][1] : strlen($value);

                                        // Extract the section from start to end
                                        $section = substr($value, $startPos, $endPos - $startPos);

                                        // Split by whitespace characters (space, newline, tab, etc.)
                                        $parts = preg_split('/\s+/', trim($section), -1, PREG_SPLIT_NO_EMPTY);

                                        // First part should be the code, rest is description
                                        if (count($parts) > 1) {
                                            array_shift($parts); // Remove the code part
                                            $description = implode(' ', $parts);
                                            
                                            // Remove trailing sorting numbers (e.g., "餐館業 2" -> "餐館業")
                                            $description = preg_replace('/\s+\d+$/', '', $description);

                                            $businessItems[] = [
                                                $code,
                                                trim($description)
                                            ];
                                        }
                                    }
                                }
                                // If no structured data found, store as raw data
                                $data[$key] = !empty($businessItems) ? $businessItems : ['raw_data' => $value];
                                break;
                        default:
                            // Parse dates
                            if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                                $data[$key] = [
                                    'year' => intval($matches[1]) + 1911,
                                    'month' => intval($matches[2]),
                                    'day' => intval($matches[3])
                                ];
                            } else {
                                $data[$key] = $value;
                            }
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

        // Ensure ID is 8 digits with leading zeros
        $id = str_pad($id, 8, '0', STR_PAD_LEFT);

        // Get first character for directory organization
        $firstChar = substr($id, 0, 1);
        $dataDir = dirname(__DIR__) . '/data/gcis/companies/details/' . $firstChar;
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }

        $filename = "{$id}.json";
        $filepath = "{$dataDir}/{$filename}";

        // Clean data recursively to ensure JSON encoding works
        $cleanData = $this->cleanDataForJson($data);

        // Add the id field to match existing structure
        $cleanData['id'] = $id;

        // Use flat structure to match existing JSON files (no metadata wrapper)
        $saveData = $cleanData;

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
        // Ensure ID is 8 digits with leading zeros
        $id = str_pad($id, 8, '0', STR_PAD_LEFT);

        // Get first character for directory organization
        $firstChar = substr($id, 0, 1);
        $dataDir = dirname(__DIR__) . '/data/gcis/businesses/details/' . $firstChar;
        if (!is_dir($dataDir)) {
            mkdir($dataDir, 0755, true);
        }

        $filename = "{$id}.json";
        $filepath = "{$dataDir}/{$filename}";

        // Add the id field to match existing structure
        $data['id'] = $id;

        // Use flat structure to match existing JSON files (no metadata wrapper)
        $saveData = $data;

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
                    $items[] = [$code, $description];
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
        // Ensure proper UTF-8 encoding
        if (!mb_check_encoding($value, 'UTF-8')) {
            $value = mb_convert_encoding($value, 'UTF-8', 'auto');
        }

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
                                if (in_array($key, ['登記現況', '公司名稱', '公司所在地'])) {
                                    $value = $this->trimKeyField($value);
                                }

                                // Parse dates
                                if (preg_match('/^(\d+)年(\d+)月(\d+)日$/', $value, $matches)) {
                                    $data[$key] = [
                                        'year' => intval($matches[1]) + 1911,
                                        'month' => intval($matches[2]),
                                        'day' => intval($matches[3])
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
        $pluralType = $type === 'company' ? 'companies' : 'businesses';
        $dataDir = dirname(__DIR__) . "/data/raw/{$pluralType}";
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
        $filepath = $this->getJsonPath($id, $type);

        if (!file_exists($filepath)) {
            return false;
        }

        // Try to read and parse the JSON file to check crawled_at field
        $jsonContent = file_get_contents($filepath);
        if ($jsonContent === false) {
            return false;
        }

        $data = json_decode($jsonContent, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            // If JSON is invalid, delete the file and return false to trigger re-crawl
            $this->logger->warning("Invalid JSON in {$filepath}, deleting corrupted file: " . json_last_error_msg());
            unlink($filepath);
            return false;
        }

        // Check if crawled_at field exists and is valid
        if (isset($data['crawled_at']) && !empty($data['crawled_at'])) {
            $crawledTime = strtotime($data['crawled_at']);
            if ($crawledTime !== false) {
                $currentTime = time();
                $hoursDiff = ($currentTime - $crawledTime) / 3600;
                return $hoursDiff < 24;
            }
        }

        // If crawled_at field doesn't exist or is invalid, fall back to file modification time
        $fileModTime = filemtime($filepath);
        $currentTime = time();
        $hoursDiff = ($currentTime - $fileModTime) / 3600;

        return $hoursDiff < 24;
    }

    private function getRawHtmlPath(string $id, string $type, string $suffix = ''): string
    {
        $pluralType = $type === 'company' ? 'companies' : 'businesses';
        $dataDir = dirname(__DIR__) . "/data/raw/{$pluralType}";
        $filename = "{$id}{$suffix}.html";
        return "{$dataDir}/{$filename}";
    }

    private function getJsonPath(string $id, string $type): string
    {
        $pluralType = $type === 'company' ? 'companies' : 'businesses';
        $id = str_pad($id, 8, '0', STR_PAD_LEFT);
        $firstChar = substr($id, 0, 1);
        $dataDir = dirname(__DIR__) . "/data/gcis/{$pluralType}/details/{$firstChar}";
        return "{$dataDir}/{$id}.json";
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
