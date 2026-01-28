// Search API types

export interface SearchRequest {
  vector: number[]
  k: number
  ef?: number
}

export interface SearchResultItem {
  id: number
  distance: number
}

export interface MergedSearchResultItem {
  id: number
  distance: number
  indexId: string
}

export interface SearchResponse {
  indexId: string
  results: SearchResultItem[]
  queryTimeMs: number
}

export interface MultiSearchRequest {
  vector: number[]
  k: number
  ef?: number
  indexIds?: string[]
}

export interface MultiSearchResponse {
  results: Record<string, SearchResultItem[]>
  merged: MergedSearchResultItem[]
  totalTimeMs: number
}

export interface BatchQueryItem {
  vector: number[]
  k: number
}

export interface BatchSearchRequest {
  queries: BatchQueryItem[]
  indexId: string
  ef?: number
}

export interface BatchSearchResultItem {
  queryIndex: number
  results: SearchResultItem[]
}

export interface BatchSearchResponse {
  results: BatchSearchResultItem[]
  totalTimeMs: number
}

// Index Management API types

export interface LoadIndexRequest {
  indexId: string
  indexPath: string
}

export interface IndexConfig {
  m?: number
  efConstruction?: number
  distanceType?: string
}

export interface CreateIndexRequest {
  indexId: string
  vectors: VectorData[]
  config?: IndexConfig
}

export interface AddVectorsRequest {
  vectors: VectorData[]
}

export interface SaveIndexRequest {
  path: string
}

export interface VectorData {
  id: number
  vector: number[]
}

export interface IndexInfo {
  indexId: string
  dimension: number
  size: number
  indexPath?: string
  distanceType?: string
}

export interface IndexListResponse {
  indexes: IndexInfo[]
  totalIndexes: number
  totalVectors: number
}

export interface IndexOperationResponse {
  success: boolean
  message: string
  index?: IndexInfo
}

// Health & Status API types

export interface HealthResponse {
  status: string
  version: string
  indexCount: number
  totalVectors: number
}

export interface ReadinessResponse {
  ready: boolean
}

export interface LivenessResponse {
  alive: boolean
}

export interface ErrorResponse {
  error: string
  message: string
}
