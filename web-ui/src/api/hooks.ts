import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from './client'
import type {
  HealthResponse,
  IndexListResponse,
  IndexInfo,
  IndexOperationResponse,
  CreateIndexRequest,
  LoadIndexRequest,
  AddVectorsRequest,
  SaveIndexRequest,
  SearchRequest,
  SearchResponse,
  MultiSearchRequest,
  MultiSearchResponse,
  BatchSearchRequest,
  BatchSearchResponse,
} from '@/types/api'

// Health & Status hooks

export function useHealth() {
  return useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: async () => {
      const { data } = await apiClient.get('/health')
      return data
    },
    refetchInterval: 10000,
  })
}

// Index Management hooks

export function useIndexes() {
  return useQuery<IndexListResponse>({
    queryKey: ['indexes'],
    queryFn: async () => {
      const { data } = await apiClient.get('/indexes')
      return data
    },
  })
}

export function useIndex(indexId: string) {
  return useQuery<IndexInfo>({
    queryKey: ['indexes', indexId],
    queryFn: async () => {
      const { data } = await apiClient.get(`/indexes/${indexId}`)
      return data
    },
    enabled: !!indexId,
  })
}

export function useCreateIndex() {
  const queryClient = useQueryClient()
  return useMutation<IndexOperationResponse, Error, CreateIndexRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post('/indexes', request)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] })
      queryClient.invalidateQueries({ queryKey: ['health'] })
    },
  })
}

export function useLoadIndex() {
  const queryClient = useQueryClient()
  return useMutation<IndexOperationResponse, Error, LoadIndexRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post('/indexes/load', request)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] })
      queryClient.invalidateQueries({ queryKey: ['health'] })
    },
  })
}

export function useDeleteIndex() {
  const queryClient = useQueryClient()
  return useMutation<IndexOperationResponse, Error, string>({
    mutationFn: async (indexId) => {
      const { data } = await apiClient.delete(`/indexes/${indexId}`)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] })
      queryClient.invalidateQueries({ queryKey: ['health'] })
    },
  })
}

export function useAddVectors(indexId: string) {
  const queryClient = useQueryClient()
  return useMutation<IndexOperationResponse, Error, AddVectorsRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post(`/indexes/${indexId}/vectors`, request)
      return data
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] })
      queryClient.invalidateQueries({ queryKey: ['health'] })
    },
  })
}

export function useSaveIndex(indexId: string) {
  return useMutation<IndexOperationResponse, Error, SaveIndexRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post(`/indexes/${indexId}/save`, request)
      return data
    },
  })
}

// Search hooks

export function useSearch(indexId: string) {
  return useMutation<SearchResponse, Error, SearchRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post(`/indexes/${indexId}/search`, request)
      return data
    },
  })
}

export function useMultiSearch() {
  return useMutation<MultiSearchResponse, Error, MultiSearchRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post('/search', request)
      return data
    },
  })
}

export function useBatchSearch() {
  return useMutation<BatchSearchResponse, Error, BatchSearchRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post('/search/batch', request)
      return data
    },
  })
}
