import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { SearchForm } from './SearchForm'
import { ResultsTable } from './ResultsTable'
import { useIndexes, useMultiSearch } from '@/api/hooks'
import type { MultiSearchResponse } from '@/types/api'

export function MultiSearch() {
  const { data: indexList } = useIndexes()
  const [vectorInput, setVectorInput] = useState('')
  const [k, setK] = useState('10')
  const [ef, setEf] = useState('')
  const [results, setResults] = useState<MultiSearchResponse | null>(null)
  const [selectedIndexIds, setSelectedIndexIds] = useState<string[]>([])

  const multiSearchMutation = useMultiSearch()

  const toggleIndex = (indexId: string) => {
    setSelectedIndexIds((prev) =>
      prev.includes(indexId)
        ? prev.filter((id) => id !== indexId)
        : [...prev, indexId]
    )
  }

  const handleSearch = () => {
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

      multiSearchMutation.mutate(
        {
          vector,
          k: parseInt(k) || 10,
          ef: ef ? parseInt(ef) : undefined,
          indexIds: selectedIndexIds.length > 0 ? selectedIndexIds : undefined,
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
          <CardTitle>Multi-Index Search</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-2">
            <label className="text-sm font-medium">
              Select Indexes (optional, searches all if none selected)
            </label>
            <div className="flex flex-wrap gap-2">
              {indexList?.indexes.map((index) => (
                <button
                  key={index.indexId}
                  onClick={() => toggleIndex(index.indexId)}
                  className={`rounded-md border px-3 py-1 text-sm transition-colors ${
                    selectedIndexIds.includes(index.indexId)
                      ? 'border-primary bg-primary text-primary-foreground'
                      : 'border-input bg-background hover:bg-accent'
                  }`}
                >
                  {index.indexId}
                </button>
              ))}
            </div>
            {selectedIndexIds.length > 0 && (
              <button
                onClick={() => setSelectedIndexIds([])}
                className="text-xs text-muted-foreground hover:underline"
              >
                Clear selection (search all)
              </button>
            )}
          </div>

          <SearchForm
            vectorInput={vectorInput}
            setVectorInput={setVectorInput}
            k={k}
            setK={setK}
            ef={ef}
            setEf={setEf}
            onSearch={handleSearch}
            isLoading={multiSearchMutation.isPending}
          />
        </CardContent>
      </Card>

      {results && (
        <>
          <ResultsTable
            results={results.merged}
            queryTimeMs={results.totalTimeMs}
            showIndexId
            title="Merged Results (Top-K)"
          />

          {Object.entries(results.results).map(([indexId, indexResults]) => (
            <ResultsTable
              key={indexId}
              results={indexResults}
              title={`Results from ${indexId}`}
            />
          ))}
        </>
      )}
    </div>
  )
}
