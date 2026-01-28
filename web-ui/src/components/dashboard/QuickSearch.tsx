import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useIndexes, useSearch } from '@/api/hooks'
import { Search } from 'lucide-react'

export function QuickSearch() {
  const navigate = useNavigate()
  const { data: indexList } = useIndexes()
  const [selectedIndex, setSelectedIndex] = useState<string>('')
  const [vectorInput, setVectorInput] = useState('')
  const [k, setK] = useState('10')

  const searchMutation = useSearch(selectedIndex)

  const handleSearch = () => {
    if (!selectedIndex || !vectorInput) return

    try {
      const vector = JSON.parse(vectorInput)
      if (!Array.isArray(vector)) {
        alert('Vector must be an array of numbers')
        return
      }
      searchMutation.mutate(
        { vector, k: parseInt(k) || 10 },
        {
          onSuccess: () => {
            navigate('/search')
          },
        }
      )
    } catch {
      alert('Invalid JSON format for vector')
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Search className="h-5 w-5" />
          Quick Search
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <label className="text-sm font-medium">Index</label>
          <Select value={selectedIndex} onValueChange={setSelectedIndex}>
            <SelectTrigger>
              <SelectValue placeholder="Select an index" />
            </SelectTrigger>
            <SelectContent>
              {indexList?.indexes.map((index) => (
                <SelectItem key={index.indexId} value={index.indexId}>
                  {index.indexId} ({index.size} vectors)
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div>
          <label className="text-sm font-medium">Vector (JSON array)</label>
          <Textarea
            placeholder="[0.1, 0.2, 0.3, ...]"
            value={vectorInput}
            onChange={(e) => setVectorInput(e.target.value)}
            className="font-mono text-sm"
          />
        </div>
        <div>
          <label className="text-sm font-medium">K (results)</label>
          <Input
            type="number"
            value={k}
            onChange={(e) => setK(e.target.value)}
            min="1"
            max="1000"
          />
        </div>
        <Button
          onClick={handleSearch}
          disabled={!selectedIndex || !vectorInput || searchMutation.isPending}
          className="w-full"
        >
          {searchMutation.isPending ? 'Searching...' : 'Search'}
        </Button>
      </CardContent>
    </Card>
  )
}
