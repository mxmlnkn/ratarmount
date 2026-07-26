#!/usr/bin/env node
'use strict';

/**
 * Build a synthetic multi-nested 7z regression fixture (no customer data).
 *
 * Structure mirrors support bundles: outer Copy 7z embedding several solid LZMA2
 * inner 7z archives, including one large CM-Secondary-like tree with many logs.
 *
 * Requires: 7z on PATH (Rocky 8 / WSL).
 *
 * Usage:
 *   node core/scripts/build-nested-7z-fixture.js [output-dir]
 *
 * Env:
 *   NESTED_FIXTURE_SECONDARY_FILES   default 120
 *   NESTED_FIXTURE_SECONDARY_FILE_BYTES default 524288 (512 KiB each)
 *   NESTED_FIXTURE_INNER_COUNT       default 4 (CM-Primary, CM-Secondary, RA, Slave)
 */

const childProcess = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const SCRIPT_DIR = path.dirname(__filename);
const CORE_ROOT = path.resolve(SCRIPT_DIR, '..');
const DEFAULT_OUT = path.join(CORE_ROOT, 'tests', 'fixtures', 'nested-7z-multi');
const OUTER_NAME = 'nested-multi-support.7z';

const SECONDARY_FILES = parseInt(process.env.NESTED_FIXTURE_SECONDARY_FILES || '80', 10);
const SECONDARY_FILE_BYTES = parseInt(process.env.NESTED_FIXTURE_SECONDARY_FILE_BYTES || '1048576', 10);

const INNER_ARCHIVES = [
  {
    id: 'cm-primary',
    name: 'fixture-support-zip--CM-Primary--10.0.0.1--2026-01-01--00-00-00.7z',
    roots: ['cmd-free.txt', 'cmd-mount.txt', 'cmd-top.txt', 'afa.ks.checksum.txt', 'NetworkSensorService.checksum.txt'],
    build(stageDir) {
      writeText(path.join(stageDir, 'cmd-free.txt'), syntheticText('cmd-free', 8192));
      writeText(path.join(stageDir, 'cmd-mount.txt'), syntheticText('cmd-mount', 8192));
      writeText(path.join(stageDir, 'cmd-top.txt'), syntheticText('cmd-top', 16384));
      writeText(path.join(stageDir, 'afa.ks.checksum.txt'), syntheticText('afa-checksum', 4096));
      writeText(path.join(stageDir, 'NetworkSensorService.checksum.txt'), syntheticText('netsensor-checksum', 4096));
    },
    probeMembers: ['cmd-top.txt'],
  },
  {
    id: 'cm-secondary',
    name: 'fixture-support-zip--CM-Secondary--10.0.0.2--2026-01-01--00-00-00.7z',
    roots: ['home'],
    build(stageDir) {
      const afa = path.join(stageDir, 'home', 'afa');
      fs.mkdirSync(path.join(afa, '.fa'), { recursive: true });
      writeText(path.join(afa, '.fa', 'config'), syntheticText('fa-config', 4096));
      writeText(path.join(afa, '.bash_history'), syntheticText('bash-history', 16384));
      writeText(path.join(afa, '.fa-distribution-flows.log'), syntheticText('flows-main', 65536));
      writeBlob(path.join(afa, '.fa-distribution-logs.history'), 'history-main', SECONDARY_FILE_BYTES);
      for (let i = 1; i <= 20; i += 1) {
        writeBlob(
          path.join(afa, '.fa-distribution-logs.history.' + i),
          'history-' + i,
          SECONDARY_FILE_BYTES,
        );
        writeBlob(
          path.join(afa, '.fa-distribution-flows.log.' + i),
          'flows-' + i,
          Math.max(32768, SECONDARY_FILE_BYTES / 4),
        );
        writeBlob(
          path.join(afa, '.fa-auto-remove.log.' + i),
          'auto-remove-' + i,
          Math.max(16384, SECONDARY_FILE_BYTES / 8),
        );
      }
      writeBlob(path.join(afa, 'monitor.log'), 'monitor-main', SECONDARY_FILE_BYTES);
      for (let i = 1; i <= 10; i += 1) {
        writeBlob(path.join(afa, 'monitor.log.' + i), 'monitor-' + i, SECONDARY_FILE_BYTES / 2);
      }
      let extra = 0;
      while (extra < SECONDARY_FILES) {
        const name = 'synthetic-log-' + String(extra).padStart(4, '0') + '.log';
        writeBlob(path.join(afa, name), name, Math.max(8192, SECONDARY_FILE_BYTES / 16));
        extra += 1;
      }
    },
    probeMembers: [
      'home/afa/.fa-distribution-logs.history',
      'home/afa/.fa-distribution-logs.history.20',
      'home/afa/monitor.log.10',
    ],
  },
  {
    id: 'ra',
    name: 'fixture-support-zip--RA--10.0.0.3--2026-01-01--00-00-00.7z',
    roots: ['data', 'home'],
    build(stageDir) {
      writeText(
        path.join(stageDir, 'data', 'algosec-ms', 'logs', 'device-cleanup.log'),
        syntheticText('device-cleanup', 131072),
      );
      writeText(
        path.join(stageDir, 'data', 'algosec-ms', 'logs', 'ms-backuprestore.access_log.2026-01-01.txt'),
        syntheticText('ms-backup', 131072),
      );
      writeText(path.join(stageDir, 'home', 'afa', '.fa', 'performance.json'), syntheticJson('performance', 8192));
      writeText(path.join(stageDir, 'home', 'afa', '.fa', 'users_info.xml'), syntheticXml('users', 8192));
    },
    probeMembers: ['home/afa/.fa/performance.json'],
  },
  {
    id: 'slave',
    name: 'fixture-support-zip--Slave--10.0.0.4--2026-01-01--00-00-00.7z',
    roots: ['var', 'data', 'home'],
    build(stageDir) {
      writeText(path.join(stageDir, 'var', 'log', 'httpd', 'error_log'), syntheticText('httpd-error', 131072));
      writeText(
        path.join(stageDir, 'var', 'log', 'algosec_toolbox', 'nas-setup.log'),
        syntheticText('nas-setup', 65536),
      );
      writeText(
        path.join(stageDir, 'data', 'algosec-ms', 'logs', 'ms-backuprestore.access_log.2026-01-01.txt'),
        syntheticText('slave-backup', 65536),
      );
      writeText(path.join(stageDir, 'home', 'afa', '.fa-auto-remove.log.2'), syntheticText('slave-auto-2', 65536));
      writeText(path.join(stageDir, 'home', 'afa', '.fa-auto-remove.log.3'), syntheticText('slave-auto-3', 65536));
    },
    probeMembers: ['home/afa/.fa-auto-remove.log.3'],
  },
];

function run(cmd, args, opts) {
  childProcess.execFileSync(cmd, args, { stdio: 'inherit', ...opts });
}

function ensure7z() {
  childProcess.execFileSync('7z', ['-h'], { stdio: 'ignore' });
}

function sha256File(filePath) {
  const hash = crypto.createHash('sha256');
  hash.update(fs.readFileSync(filePath));
  return hash.digest('hex');
}

function syntheticText(label, size) {
  const header = '# synthetic fixture label=' + label + ' bytes=' + size + '\n';
  const line = '[' + label + '] level=INFO ts=2026-01-01T00:00:00Z msg=repeatable fixture payload\n';
  let out = header;
  while (out.length < size) {
    out += line;
  }
  return out.slice(0, size);
}

function syntheticJson(label, size) {
  const chunk = '{"label":"' + label + '","values":[' + Array(32).join('1,') + '1]}\n';
  let out = '[';
  while (out.length < size) {
    out += chunk;
  }
  return out.slice(0, size);
}

function syntheticXml(label, size) {
  const chunk = '<entry label="' + label + '"><value>fixture</value></entry>\n';
  let out = '<?xml version="1.0"?><root>\n';
  while (out.length < size) {
    out += chunk;
  }
  return out.slice(0, size);
}

function writeText(filePath, content) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

function syntheticBlob(label, size) {
  const seed = crypto.createHash('sha256').update(label).digest();
  const buf = Buffer.alloc(size);
  for (let i = 0; i < size; i += 1) {
    buf[i] = seed[i % seed.length] ^ ((i * 1103515245 + 12345) >>> 16);
  }
  return buf;
}

function writeBlob(filePath, label, size) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, syntheticBlob(label, size));
}

function countFiles(rootDir) {
  let count = 0;
  const stack = [rootDir];
  while (stack.length) {
    const current = stack.pop();
    for (const entry of fs.readdirSync(current)) {
      const full = path.join(current, entry);
      const st = fs.statSync(full);
      if (st.isDirectory()) {
        stack.push(full);
      } else {
        count += 1;
      }
    }
  }
  return count;
}

function buildInnerArchive(workDir, inner) {
  const stageDir = path.join(workDir, 'stage-' + inner.id);
  fs.rmSync(stageDir, { recursive: true, force: true });
  fs.mkdirSync(stageDir, { recursive: true });
  inner.build(stageDir);
  const innerOut = path.join(workDir, inner.name);
  const addPaths = inner.roots.map(function (root) {
    return path.join(stageDir, root);
  });
  console.log('Creating inner', inner.name, '(' + countFiles(stageDir) + ' files staged)');
  run('7z', ['a', '-y', '-t7z', '-mx=5', '-m0=LZMA2', '-ms=on', innerOut, ...addPaths]);
  return innerOut;
}

function writeManifest(outDir, innerMeta, outerPath) {
  const manifest = {
    source: 'synthetic multi-nested 7z fixture (no customer data)',
    secondary_files: SECONDARY_FILES,
    secondary_file_bytes: SECONDARY_FILE_BYTES,
    inner_archives: innerMeta,
    files: {
      outer_7z: path.basename(outerPath),
      outer_size_bytes: fs.statSync(outerPath).size,
      outer_sha256: sha256File(outerPath),
    },
  };
  fs.writeFileSync(path.join(outDir, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
}

function main() {
  const outDir = path.resolve(process.argv[2] || DEFAULT_OUT);
  const workDir = path.join(outDir, '.build');
  const outerOut = path.join(outDir, OUTER_NAME);

  ensure7z();
  fs.mkdirSync(outDir, { recursive: true });
  fs.rmSync(workDir, { recursive: true, force: true });
  fs.mkdirSync(workDir, { recursive: true });

  const innerOutputs = [];
  const innerMeta = [];
  for (const inner of INNER_ARCHIVES) {
    const innerPath = buildInnerArchive(workDir, inner);
    innerOutputs.push(innerPath);
    innerMeta.push({
      id: inner.id,
      name: inner.name,
      probe_members: inner.probeMembers,
      size_bytes: fs.statSync(innerPath).size,
      sha256: sha256File(innerPath),
      staged_file_count: countFiles(path.join(workDir, 'stage-' + inner.id)),
    });
  }

  writeText(
    path.join(workDir, 'collect-support-zip.log'),
    'synthetic nested-multi-support fixture\n',
  );

  console.log('Creating outer nested 7z with', innerOutputs.length, 'embedded archives');
  run('7z', ['a', '-y', '-t7z', '-mx=3', '-m0=Copy', outerOut, ...innerOutputs, path.join(workDir, 'collect-support-zip.log')]);

  writeManifest(outDir, innerMeta, outerOut);
  fs.rmSync(workDir, { recursive: true, force: true });

  console.log('Wrote', outerOut, '(' + fs.statSync(outerOut).size + ' bytes)');
  for (const item of innerMeta) {
    console.log(' ', item.name, item.size_bytes, 'bytes', item.staged_file_count, 'files');
  }
}

main();
