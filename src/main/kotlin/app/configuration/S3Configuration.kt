package app.configuration

import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
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

    /*@Bean
    @Profile("localS3")
    fun localS3(): AmazonS3 {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region))
                .withClientConfiguration(ClientConfiguration().withProtocol(Protocol.HTTP))
                .withCredentials(
                        AWSStaticCredentialsProvider(BasicAWSCredentials(awsAccessKey, awsSecretKey)))
                .withPathStyleAccessEnabled(true)
                .disableChunkedEncoding()
                .build()
    }*/

    @Value("\${aws.region}")
    private lateinit var region: String

   /* @Value("\${s3.service.endpoint}")
    private lateinit var serviceEndpoint: String

    @Value("\${aws.access.key}")
    private lateinit var awsAccessKey: String

    @Value("\${aws.secret.key}")
    private lateinit var awsSecretKey: String*/
}
