import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Stream;

public class Task1 {

    private static final String INDEX = "{" +
            "  \"settings\": {" +
            "    \"number_of_shards\": 1" +
            "  }," +
            "  \"mappings\": {" +
            "    \"product\": {" +
            "      \"properties\": {" +
            "        \"productClass\": {" +
            "          \"type\": \"keyword\"" +
            "        }," +
            "        \"productSubclass\": {" +
            "          \"type\": \"keyword\"" +
            "        }," +
            "        \"department\": {" +
            "          \"type\": \"keyword\"" +
            "        }" +
            "      }" +
            "    }" +
            "  }" +
            "}";

    private static final RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

    public static void main(String[] args) throws IOException, URISyntaxException {
        dropIndex();
        initIndex();

        Path productsFolder = Paths.get(Task1.class.getClassLoader().getResource("products").toURI());
        Files.walk(productsFolder)
                .filter(Files::isRegularFile)
                .flatMap(Task1::fileToProduct)
                .forEach(Task1::index)
        ;
    }

    private static boolean indexExists() throws IOException {
        Response response = restClient.performRequest("HEAD", "/products");
        return response.getStatusLine().getStatusCode() == 200;
    }

    private static void dropIndex() throws IOException {
        if (indexExists()) {
            restClient.performRequest("DELETE", "/products");
        }
    }

    private static void initIndex() throws IOException {
        HttpEntity entity = new NStringEntity(INDEX, ContentType.APPLICATION_JSON);
        restClient.performRequest("PUT", "/products", Collections.emptyMap(), entity);
    }

    private static Stream<ProductRecordRoot> fileToProduct(Path file) {
        try {
            JAXBContext jc = JAXBContext.newInstance(ProductRecords.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            ProductRecords productRecords = (ProductRecords) unmarshaller.unmarshal(file.toFile());
            return productRecords.getProducts().stream();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    private static void index(ProductRecordRoot product) {
        try {
            HttpEntity entity = new NStringEntity(parseToJsonString(product), ContentType.APPLICATION_JSON);
            restClient.performRequest("PUT", "/products/product/" + getIdFromProduct(product), Collections.emptyMap(), entity);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static String getIdFromProduct(ProductRecordRoot product) {
        return product.productId;
    }

    private static String parseToJsonString(ProductRecordRoot product) {
        return new Gson().toJson(product);
    }

}
