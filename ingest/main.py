from src.pipelines import ArtBlocksSalesPipeline, CryptoPunkSalesPipeline, NounsDaoAuctionPipeline


if __name__ == "__main__":
    ArtBlocksSalesPipeline().run()
    CryptoPunkSalesPipeline().run()
    NounsDaoAuctionPipeline().run()
