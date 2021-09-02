from src.pipelines import ArtBlocksSalesPipeline, CryptoPunkSalesPipeline


if __name__ == "__main__":
    ArtBlocksSalesPipeline().run()
    CryptoPunkSalesPipeline().run()
