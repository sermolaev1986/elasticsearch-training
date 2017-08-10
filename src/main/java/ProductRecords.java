import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "products")
class ProductRecords {

    private List<ProductRecordRoot> products = new ArrayList<>();

    @XmlElement(name = "product")
    public List<ProductRecordRoot> getProducts() {
        return products;
    }
}
