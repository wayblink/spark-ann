import { useState, useEffect } from 'react'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Button } from '@/components/ui/button'
import { Search, Sparkles } from 'lucide-react'

function generateRandomVector(dimension: number): number[] {
  const vector: number[] = []
  for (let i = 0; i < dimension; i++) {
    vector.push(Math.random() * 2 - 1) // Random values between -1 and 1
  }
  return vector
}

interface SearchFormProps {
  vectorInput: string
  setVectorInput: (value: string) => void
  k: string
  setK: (value: string) => void
  ef: string
  setEf: (value: string) => void
  onSearch: () => void
  isLoading: boolean
  disabled?: boolean
  defaultDimension?: number
}

export function SearchForm({
  vectorInput,
  setVectorInput,
  k,
  setK,
  ef,
  setEf,
  onSearch,
  isLoading,
  disabled,
  defaultDimension = 128,
}: SearchFormProps) {
  const [dimension, setDimension] = useState(String(defaultDimension))

  useEffect(() => {
    if (defaultDimension) {
      setDimension(String(defaultDimension))
    }
  }, [defaultDimension])

  const handleGenerateVector = () => {
    const dim = parseInt(dimension) || 128
    const vector = generateRandomVector(dim)
    setVectorInput(JSON.stringify(vector))
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-2">
        <div className="flex items-center justify-between">
          <label className="text-sm font-medium">Query Vector (JSON array)</label>
          <div className="flex items-center gap-2">
            <Input
              type="number"
              value={dimension}
              onChange={(e) => setDimension(e.target.value)}
              className="w-20 h-8 text-xs"
              min="1"
              max="2048"
              placeholder="Dim"
            />
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={handleGenerateVector}
            >
              <Sparkles className="mr-1 h-3 w-3" />
              Random
            </Button>
          </div>
        </div>
        <Textarea
          placeholder="[0.1, 0.2, 0.3, 0.4, ...]"
          value={vectorInput}
          onChange={(e) => setVectorInput(e.target.value)}
          className="h-24 font-mono text-sm"
        />
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div className="grid gap-2">
          <label className="text-sm font-medium">K (results)</label>
          <Input
            type="number"
            value={k}
            onChange={(e) => setK(e.target.value)}
            min="1"
            max="1000"
          />
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
      </div>
      <Button onClick={onSearch} disabled={disabled || isLoading} className="w-full">
        <Search className="mr-2 h-4 w-4" />
        {isLoading ? 'Searching...' : 'Search'}
      </Button>
    </div>
  )
}
