import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { ResultsTable } from './ResultsTable'
import { useIndexes, useBatchSearch } from '@/api/hooks'
import { Search, Sparkles } from 'lucide-react'
import type { BatchSearchResponse, BatchQueryItem } from '@/types/api'

function generateRandomQueries(count: number, dimension: number, k: number): BatchQueryItem[] {
  const queries: BatchQueryItem[] = []
  for (let i = 0; i < count; i++) {
    const vector: number[] = []
    for (let j = 0; j < dimension; j++) {
      vector.push(Math.random() * 2 - 1)
    }
    queries.push({ vector, k })
  }
  return queries
}

export function BatchSearch() {
  const { data: indexList } = useIndexes()
  const [selectedIndex, setSelectedIndex] = useState('')
  const [queriesInput, setQueriesInput] = useState('')
  const [ef, setEf] = useState('')
  const [results, setResults] = useState<BatchSearchResponse | null>(null)
  const [queryCount, setQueryCount] = useState('5')
  const [dimension, setDimension] = useState('128')
  const [defaultK, setDefaultK] = useState('10')

  const batchSearchMutation = useBatchSearch()

  const selectedIndexInfo = indexList?.indexes.find(
    (idx) => idx.indexId === selectedIndex
  )

  useEffect(() => {
    if (selectedIndexInfo?.dimension) {
      setDimension(String(selectedIndexInfo.dimension))
    }
  }, [selectedIndexInfo?.dimension])

  const handleGenerateQueries = () => {
    const count = parseInt(queryCount) || 5
    const dim = parseInt(dimension) || 128
    const k = parseInt(defaultK) || 10
    const queries = generateRandomQueries(count, dim, k)
    setQueriesInput(JSON.stringify(queries, null, 2))
  }

  const handleSearch = () => {
    if (!selectedIndex) {
      alert('Please select an index')
      return
    }
    if (!queriesInput.trim()) {
      alert('Please enter batch queries')
      return
    }

    try {
      const queries: BatchQueryItem[] = JSON.parse(queriesInput)
      if (!Array.isArray(queries)) {
        alert('Queries must be an array')
        return
      }

      batchSearchMutation.mutate(
        {
          queries,
          indexId: selectedIndex,
          ef: ef ? parseInt(ef) : undefined,
        },
        {
          onSuccess: (data) => {
            setResults(data)
          },
          onError: (error) => {
            alert(`Batch search failed: ${error.message}`)
          },
        }
      )
    } catch {
      alert('Invalid JSON format for queries')
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Batch Search</CardTitle>
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

          <div className="grid gap-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">
                Batch Queries (JSON array)
              </label>
              <div className="flex items-center gap-2">
                <Input
                  type="number"
                  value={queryCount}
                  onChange={(e) => setQueryCount(e.target.value)}
                  className="w-16 h-8 text-xs"
                  min="1"
                  max="100"
                  title="Number of queries"
                />
                <span className="text-xs text-muted-foreground">x</span>
                <Input
                  type="number"
                  value={dimension}
                  onChange={(e) => setDimension(e.target.value)}
                  className="w-16 h-8 text-xs"
                  min="1"
                  max="2048"
                  title="Dimension"
                />
                <span className="text-xs text-muted-foreground">k=</span>
                <Input
                  type="number"
                  value={defaultK}
                  onChange={(e) => setDefaultK(e.target.value)}
                  className="w-16 h-8 text-xs"
                  min="1"
                  max="1000"
                  title="K per query"
                />
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={handleGenerateQueries}
                >
                  <Sparkles className="mr-1 h-3 w-3" />
                  Generate
                </Button>
              </div>
            </div>
            <Textarea
              placeholder={`[
  {"vector": [0.1, 0.2, ...], "k": 10},
  {"vector": [0.3, 0.4, ...], "k": 5}
]`}
              value={queriesInput}
              onChange={(e) => setQueriesInput(e.target.value)}
              className="h-40 font-mono text-sm"
            />
            <p className="text-xs text-muted-foreground">
              Format: {`[{"vector": number[], "k": number}, ...]`}
            </p>
          </div>

          <div className="grid gap-2">
            <label className="text-sm font-medium">EF (optional)</label>
            <Input
              type="number"
              value={ef}
              onChange={(e) => setEf(e.target.value)}
              min="1"
              max="1000"
              placeholder="Default: 50"
            />
          </div>

          <Button
            onClick={handleSearch}
            disabled={!selectedIndex || batchSearchMutation.isPending}
            className="w-full"
          >
            <Search className="mr-2 h-4 w-4" />
            {batchSearchMutation.isPending ? 'Searching...' : 'Run Batch Search'}
          </Button>
        </CardContent>
      </Card>

      {results && (
        <>
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Batch Summary</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-muted-foreground">
                {results.results.length} queries executed in {results.totalTimeMs}ms
              </p>
            </CardContent>
          </Card>

          {results.results.map((queryResult) => (
            <ResultsTable
              key={queryResult.queryIndex}
              results={queryResult.results}
              title={`Query ${queryResult.queryIndex + 1} Results`}
            />
          ))}
        </>
      )}
    </div>
  )
}
