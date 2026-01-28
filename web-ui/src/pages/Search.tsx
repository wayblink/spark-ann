import { Header } from '@/components/layout/Header'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { SingleSearch } from '@/components/search/SingleSearch'
import { MultiSearch } from '@/components/search/MultiSearch'
import { BatchSearch } from '@/components/search/BatchSearch'

export function Search() {
  return (
    <div className="flex flex-col">
      <Header title="Vector Search" />
      <div className="flex-1 p-6">
        <Tabs defaultValue="single" className="space-y-6">
          <TabsList>
            <TabsTrigger value="single">Single Index</TabsTrigger>
            <TabsTrigger value="multi">Multi-Index</TabsTrigger>
            <TabsTrigger value="batch">Batch</TabsTrigger>
          </TabsList>

          <TabsContent value="single">
            <SingleSearch />
          </TabsContent>

          <TabsContent value="multi">
            <MultiSearch />
          </TabsContent>

          <TabsContent value="batch">
            <BatchSearch />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
