import { describe, expect, it, vi } from 'vitest';
import {
  athenaConnectionConfigFromConfig,
  isKtxAthenaConnectionConfig,
  KtxAthenaScanConnector,
  type KtxAthenaClientFactory,
  type KtxAthenaClient,
  type KtxGlueClient,
} from '../../../src/connectors/athena/connector.js';
import { createAthenaLiveDatabaseIntrospection } from '../../../src/connectors/athena/live-database-introspection.js';

function fakeClientFactory(options: { queryState?: string; queryError?: string } = {}): KtxAthenaClientFactory {
  const state = options.queryState ?? 'SUCCEEDED';

  const fakeAthenaClient: KtxAthenaClient = {
    startQueryExecution: vi.fn(async () => ({ QueryExecutionId: 'exec-1' })),
    getQueryExecution: vi.fn(async () => ({
      QueryExecution: {
        Status: {
          State: state,
          StateChangeReason: options.queryError,
        },
      },
    })),
    getQueryResults: vi.fn(async () => ({
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: [
            { Name: 'id', Type: 'bigint' },
            { Name: 'status', Type: 'string' },
          ],
        },
        Rows: [
          // Header row (Athena always includes it on first page)
          { Data: [{ VarCharValue: 'id' }, { VarCharValue: 'status' }] },
          // Data row
          { Data: [{ VarCharValue: '1' }, { VarCharValue: 'paid' }] },
        ],
      },
      NextToken: undefined,
    })),
  };

  const fakeGlueClient: KtxGlueClient = {
    getDatabases: vi.fn(async () => ({
      DatabaseList: [{ Name: 'analytics' }],
      NextToken: undefined,
    })),
    getTables: vi.fn(async () => ({
      TableList: [
        {
          Name: 'orders',
          TableType: 'EXTERNAL_TABLE',
          Description: 'Orders table',
          StorageDescriptor: {
            Columns: [
              { Name: 'id', Type: 'bigint', Comment: 'Order id' },
              { Name: 'status', Type: 'string' },
            ],
          },
        },
      ],
      NextToken: undefined,
    })),
  };

  return {
    createAthenaClient: vi.fn(() => fakeAthenaClient),
    createGlueClient: vi.fn(() => fakeGlueClient),
  };
}

const connection = {
  driver: 'athena',
  region: 'us-east-1',
  s3_staging_dir: 's3://my-bucket/athena-results/',
  workgroup: 'analytics',
  catalog: 'AwsDataCatalog',
  database: 'analytics',
} as const;

describe('KtxAthenaScanConnector', () => {
  it('identifies athena connection configs correctly', () => {
    expect(isKtxAthenaConnectionConfig(connection)).toBe(true);
    expect(isKtxAthenaConnectionConfig({ driver: 'bigquery' })).toBe(false);
    expect(isKtxAthenaConnectionConfig(null)).toBe(false);
    expect(isKtxAthenaConnectionConfig(undefined)).toBe(false);
  });

  it('resolves configuration and throws on missing required fields', () => {
    expect(athenaConnectionConfigFromConfig({ connectionId: 'dw', connection })).toMatchObject({
      region: 'us-east-1',
      s3StagingDir: 's3://my-bucket/athena-results/',
      workgroup: 'analytics',
      catalog: 'AwsDataCatalog',
      database: 'analytics',
    });

    expect(() =>
      athenaConnectionConfigFromConfig({ connectionId: 'dw', connection: { driver: 'athena' } }),
    ).toThrow('connections.dw.region');

    expect(() =>
      athenaConnectionConfigFromConfig({
        connectionId: 'dw',
        connection: { driver: 'athena', region: 'us-east-1' },
      }),
    ).toThrow('connections.dw.s3_staging_dir');
  });

  it('applies defaults for optional config fields', () => {
    const resolved = athenaConnectionConfigFromConfig({
      connectionId: 'dw',
      connection: { driver: 'athena', region: 'us-east-1', s3_staging_dir: 's3://bucket/' },
    });
    expect(resolved.workgroup).toBe('primary');
    expect(resolved.catalog).toBe('AwsDataCatalog');
    expect(resolved.database).toBeUndefined();
  });

  it('introspects databases, tables, and columns from Glue', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
      now: () => new Date('2026-06-21T10:00:00.000Z'),
    });

    const snapshot = await connector.introspect(
      { connectionId: 'dw', driver: 'athena' },
      { runId: 'scan-1' },
    );

    expect(snapshot).toMatchObject({
      connectionId: 'dw',
      driver: 'athena',
      extractedAt: '2026-06-21T10:00:00.000Z',
      scope: { catalogs: ['AwsDataCatalog'], datasets: ['analytics'] },
      metadata: {
        catalog: 'AwsDataCatalog',
        databases: ['analytics'],
        table_count: 1,
        total_columns: 2,
      },
    });

    expect(snapshot.tables[0]).toMatchObject({
      catalog: 'AwsDataCatalog',
      db: 'analytics',
      name: 'orders',
      kind: 'table',
      comment: 'Orders table',
      estimatedRows: null,
      foreignKeys: [],
    });

    expect(snapshot.tables[0]?.columns).toEqual([
      {
        name: 'id',
        nativeType: 'bigint',
        normalizedType: 'BIGINT',
        dimensionType: 'number',
        nullable: true,
        primaryKey: false,
        comment: 'Order id',
      },
      {
        name: 'status',
        nativeType: 'string',
        normalizedType: 'VARCHAR',
        dimensionType: 'string',
        nullable: true,
        primaryKey: false,
        comment: null,
      },
    ]);
  });

  it('samples a table via Athena query execution', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    const result = await connector.sampleTable(
      {
        connectionId: 'dw',
        table: { catalog: 'AwsDataCatalog', db: 'analytics', name: 'orders' },
        columns: ['id', 'status'],
        limit: 10,
      },
      { runId: 'scan-1' },
    );

    expect(result).toMatchObject({
      headers: ['id', 'status'],
      rows: [['1', 'paid']],
      totalRows: 1,
    });
  });

  it('samples a column via Athena query execution', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    const result = await connector.sampleColumn(
      {
        connectionId: 'dw',
        table: { catalog: 'AwsDataCatalog', db: 'analytics', name: 'orders' },
        column: 'status',
        limit: 10,
      },
      { runId: 'scan-1' },
    );

    expect(result).toMatchObject({
      values: ['1'],
      nullCount: null,
      distinctCount: null,
    });
  });

  it('executes read-only SQL and rejects write statements', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    await expect(
      connector.executeReadOnly(
        { connectionId: 'dw', sql: 'SELECT id, status FROM "analytics"."orders"', maxRows: 100 },
        { runId: 'scan-1' },
      ),
    ).resolves.toMatchObject({
      headers: ['id', 'status'],
      rows: [['1', 'paid']],
      rowCount: 1,
    });

    await expect(
      connector.executeReadOnly({ connectionId: 'dw', sql: 'DELETE FROM orders' }, { runId: 'scan-1' }),
    ).rejects.toThrow('Only read-only SELECT/WITH queries can be executed locally');
  });

  it('lists schemas (databases) from Glue', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    await expect(connector.listSchemas()).resolves.toEqual(['analytics']);
  });

  it('lists tables from Glue', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    await expect(connector.listTables(['analytics'])).resolves.toEqual([
      {
        catalog: 'AwsDataCatalog',
        schema: 'analytics',
        name: 'orders',
        kind: 'table',
      },
    ]);
  });

  it('returns null for columnStats', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    await expect(
      connector.columnStats(
        { connectionId: 'dw', table: { catalog: 'AwsDataCatalog', db: 'analytics', name: 'orders' }, column: 'status' },
        { runId: 'scan-1' },
      ),
    ).resolves.toBeNull();
  });

  it('tests connection successfully', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });

    await expect(connector.testConnection()).resolves.toMatchObject({ success: true });
  });

  it('returns failure result when testConnection throws', async () => {
    const factory = fakeClientFactory();
    const glueClient = factory.createGlueClient('us-east-1');
    vi.mocked(glueClient.getDatabases).mockRejectedValue(new Error('Access denied'));
    const brokenFactory: KtxAthenaClientFactory = {
      createAthenaClient: factory.createAthenaClient,
      createGlueClient: vi.fn(() => glueClient),
    };

    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: brokenFactory,
    });

    await expect(connector.testConnection()).resolves.toMatchObject({
      success: false,
      error: 'Access denied',
    });
  });

  it('cleans up without throwing', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory(),
    });
    await connector.listSchemas();
    await expect(connector.cleanup()).resolves.toBeUndefined();
  });

  it('throws when query execution fails', async () => {
    const connector = new KtxAthenaScanConnector({
      connectionId: 'dw',
      connection,
      clientFactory: fakeClientFactory({ queryState: 'FAILED', queryError: 'Syntax error in SQL' }),
    });

    await expect(
      connector.executeReadOnly({ connectionId: 'dw', sql: 'SELECT 1' }, { runId: 'scan-1' }),
    ).rejects.toThrow('Athena query FAILED: Syntax error in SQL');
  });

  it('adapts to the live-database introspection port via factory', async () => {
    const introspection = createAthenaLiveDatabaseIntrospection({
      connections: { dw: connection },
      clientFactory: fakeClientFactory(),
      now: () => new Date('2026-06-21T10:00:00.000Z'),
    });

    await expect(introspection.extractSchema('dw')).resolves.toMatchObject({
      connectionId: 'dw',
      driver: 'athena',
      metadata: { catalog: 'AwsDataCatalog' },
      tables: expect.arrayContaining([
        expect.objectContaining({
          db: 'analytics',
          name: 'orders',
          columns: expect.arrayContaining([
            expect.objectContaining({ name: 'id', dimensionType: 'number' }),
          ]),
        }),
      ]),
    });
  });
});
