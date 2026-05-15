import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiClient } from './client'
import type {
  BundleInfo,
  BundleLoadRequest,
  HealthResponse,
  SearchRequest,
  SearchResponse,
  MultiSearchRequest,
  MultiSearchResponse,
  BatchSearchRequest,
  BatchSearchResponse,
  UnifiedIndexListResponse,
  UnifiedIndexEntry,
} from '@/types/api'

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

export function useIndexes() {
  return useQuery<UnifiedIndexListResponse>({
    queryKey: ['indexes'],
    queryFn: async () => {
      const { data } = await apiClient.get('/indexes')
      return data
    },
  })
}

export function useIndex(indexId: string) {
  return useQuery<UnifiedIndexEntry>({
    queryKey: ['indexes', indexId],
    queryFn: async () => {
      const { data } = await apiClient.get(`/indexes/${indexId}`)
      return data
    },
    enabled: !!indexId,
  })
}

export function useLoadBundle() {
  const queryClient = useQueryClient()
  return useMutation<BundleInfo, Error, BundleLoadRequest>({
    mutationFn: async (request) => {
      const { data } = await apiClient.post('/indexes/bundle', request)
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
  return useMutation<void, Error, string>({
    mutationFn: async (indexId) => {
      await apiClient.delete(`/indexes/${indexId}`)
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] })
      queryClient.invalidateQueries({ queryKey: ['health'] })
    },
  })
}

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
