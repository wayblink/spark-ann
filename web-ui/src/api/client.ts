import axios from 'axios'

const API_BASE_URL = '/api/v1'

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Allow runtime configuration of base URL
export function setApiBaseUrl(url: string) {
  apiClient.defaults.baseURL = url
}

export function getApiBaseUrl(): string {
  return apiClient.defaults.baseURL || API_BASE_URL
}
