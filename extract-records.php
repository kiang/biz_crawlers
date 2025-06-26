<?php

ini_set('memory_limit', '2G');

$downloadsDir = __DIR__ . '/downloads/';
$dataDir = __DIR__ . '/data/gcis/';

// Create directories for first character organization
function getFirstCharDir($id) {
    return substr($id, 0, 1);
}

function processJsonlGz($filename, $type) {
    global $dataDir;
    
    echo "Processing {$filename}...\n";
    
    // Use gzopen instead of fopen for better gz handling
    $gzFile = gzopen($filename, 'r');
    if (!$gzFile) {
        echo "Error: Cannot open {$filename}\n";
        return;
    }
    
    $lineCount = 0;
    $savedCount = 0;
    
    while (($line = gzgets($gzFile)) !== false) {
        $lineCount++;
        
        $record = json_decode(trim($line), true);
        if (!$record) {
            continue;
        }
        
        // Get ID field - try different possible ID fields
        $id = null;
        if ($type === 'companies') {
            $id = $record['統一編號'] ?? $record['id'] ?? null;
        } else if ($type === 'businesses') {
            $id = $record['商業統一編號'] ?? $record['id'] ?? null;
        }
        
        if (!$id) {
            continue;
        }
        
        // Ensure ID is 8 digits with leading zeros
        $id = str_pad($id, 8, '0', STR_PAD_LEFT);
        
        // Update the ID field in the record itself
        if ($type === 'companies') {
            if (isset($record['統一編號'])) {
                $record['統一編號'] = $id;
            }
            if (isset($record['id'])) {
                $record['id'] = $id;
            }
        } else if ($type === 'businesses') {
            if (isset($record['商業統一編號'])) {
                $record['商業統一編號'] = $id;
            }
            if (isset($record['id'])) {
                $record['id'] = $id;
            }
        }
        
        // Get first character for directory
        $firstChar = getFirstCharDir($id);
        
        // Create target directory path
        $targetDir = $dataDir . $type . '/details/' . $firstChar . '/';
        if (!is_dir($targetDir)) {
            mkdir($targetDir, 0755, true);
        }
        
        // Save individual JSON file
        $targetFile = $targetDir . $id . '.json';
        if (file_put_contents($targetFile, json_encode($record, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE))) {
            $savedCount++;
        }
        
        if ($lineCount % 1000 == 0) {
            echo "Processed {$lineCount} lines, saved {$savedCount} records...\n";
        }
    }
    
    gzclose($gzFile);
    echo "Finished {$filename}: {$lineCount} lines processed, {$savedCount} records saved\n";
}

// Process company files
$companyFiles = glob($downloadsDir . '[0-9]*.jsonl.gz');
foreach ($companyFiles as $file) {
    processJsonlGz($file, 'companies');
}

// Process business files  
$businessFiles = glob($downloadsDir . 'bussiness-*.jsonl.gz');
foreach ($businessFiles as $file) {
    processJsonlGz($file, 'businesses');
}

echo "All files processed successfully!\n";