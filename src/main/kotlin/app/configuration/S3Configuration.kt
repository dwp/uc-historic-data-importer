package app.configuration

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
class S3Configuration {

    @Bean
    @Profile("awsS3")
    fun awss3(): AmazonS3 {
        val updatedRegion = region.toUpperCase().replace("-", "_")
        val clientRegion = Regions.valueOf(updatedRegion)
        return AmazonS3ClientBuilder.standard()
                .withCredentials(DefaultAWSCredentialsProviderChain())
                .withRegion(clientRegion)
                .build()
    }

    @Value("\${aws.region}")
    private lateinit var region: String

}
