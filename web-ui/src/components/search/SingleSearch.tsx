import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { SearchForm } from './SearchForm'
import { ResultsTable } from './ResultsTable'
import { useIndexes, useSearch } from '@/api/hooks'
import type { SearchResponse } from '@/types/api'

export function SingleSearch() {
  const { data: indexList } = useIndexes()
  const [selectedIndex, setSelectedIndex] = useState('')
  const [vectorInput, setVectorInput] = useState('')
  const [k, setK] = useState('10')
  const [ef, setEf] = useState('')
  const [results, setResults] = useState<SearchResponse | null>(null)

  const searchMutation = useSearch(selectedIndex)

  const selectedIndexInfo = indexList?.indexes.find(
    (idx) => idx.indexId === selectedIndex
  )

  const handleSearch = () => {
    if (!selectedIndex) {
      alert('Please select an index')
      return
    }
    if (!vectorInput.trim()) {
      alert('Please enter a query vector')
      return
    }

    try {
      const vector = JSON.parse(vectorInput)
      if (!Array.isArray(vector)) {
        alert('Vector must be an array of numbers')
        return
      }

      searchMutation.mutate(
        {
          vector,
          k: parseInt(k) || 10,
          ef: ef ? parseInt(ef) : undefined,
        },
        {
          onSuccess: (data) => {
            setResults(data)
          },
          onError: (error) => {
            alert(`Search failed: ${error.message}`)
          },
        }
      )
    } catch {
      alert('Invalid JSON format for vector')
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Single Index Search</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-2">
            <label className="text-sm font-medium">Select Index</label>
            <Select value={selectedIndex} onValueChange={setSelectedIndex}>
              <SelectTrigger>
                <SelectValue placeholder="Choose an index" />
              </SelectTrigger>
              <SelectContent>
                {indexList?.indexes.map((index) => (
                  <SelectItem key={index.indexId} value={index.indexId}>
                    {index.indexId} ({index.dimension}D, {index.size} vectors)
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <SearchForm
            vectorInput={vectorInput}
            setVectorInput={setVectorInput}
            k={k}
            setK={setK}
            ef={ef}
            setEf={setEf}
            onSearch={handleSearch}
            isLoading={searchMutation.isPending}
            disabled={!selectedIndex}
            defaultDimension={selectedIndexInfo?.dimension}
          />
        </CardContent>
      </Card>

      {results && (
        <ResultsTable
          results={results.results}
          queryTimeMs={results.queryTimeMs}
          title={`Results from ${results.indexId}`}
        />
      )}
    </div>
  )
}
