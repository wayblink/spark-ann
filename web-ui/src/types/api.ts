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

// Bundle API types

export interface BundleLoadRequest {
  indexId: string
  bundlePath: string
}

export interface BundleInfo {
  indexId: string
  bundlePath: string
  totalVectors: number
  dimension: number
  numLocalIndexes: number
  hasGlobalIndex: boolean
  algorithm: string
  distanceType: string
  loadedAt: number
}

export interface UnifiedIndexEntry {
  kind: 'bundle'
  indexId: string
  dimension: number
  size: number
  distanceType: string
  bundlePath?: string
  numLocalIndexes?: number
  hasGlobalIndex?: boolean
  algorithm?: string
  loadedAt: number
}

export interface UnifiedIndexListResponse {
  indexes: UnifiedIndexEntry[]
  totalIndexes: number
  totalVectors: number
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
