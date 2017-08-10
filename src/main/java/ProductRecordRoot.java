import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

@XmlRootElement
class ProductRecordRoot {
	@XmlElement
	String sku;
	@XmlElement
	String productId;
	@XmlElement
	String name;
	@XmlElement
	String type;
	@XmlElement
	Date startDate;
	@XmlElement
	Date activeUpdateDate;
	@XmlElement
	Double regularPrice;
	@XmlElement
	Double salePrice;
	@XmlElement
	Boolean onSale;
	@XmlElement
	Date priceUpdateDate;
	@XmlElement
	Boolean digital;
	@XmlElement
	String productTemplate;
	@XmlElementWrapper(name = "categoryPath")
	@XmlElement(name = "category")
	List<ProductCategory> categories;
	@XmlElement
	Long customerReviewCount;
	@XmlElement
	Double customerReviewAverage;
	@XmlElement
	Boolean customerTopRated;
	@XmlElement
	Boolean freeShipping;
	@XmlElement
	Boolean freeShippingEligible;
	@XmlElement
	Boolean inStoreAvailability;
	@XmlElement
	String inStoreAvailabilityText;
	@XmlElement
	Date inStoreAvailabilityUpdateDate;
	@XmlElement
	Date itemUpdateDate;
	@XmlElement
	Double shippingCost;
	@XmlElement
	Boolean specialOrder;
	@XmlElement
	String shortDescription;
	@XmlElement(name = "class")
	String productClass;
	@XmlElement(name = "subclass")
	String productSubclass;
	@XmlElement
	String department;
	@XmlElement
	String manufacturer;
	@XmlElement
	Double weight;
	@XmlElement
	Double shippingWeight;
	@XmlElement
	String longDescription;
	@XmlElementWrapper(name = "includedItemList")
	@XmlElement(name = "includedItem")
	List<String> includedItemList;
	@XmlElementWrapper(name = "features")
	@XmlElement(name = "feature")
	List<String> features;
	@XmlElement
	Long quantityLimit;
	@XmlElementWrapper(name = "bundledIn")
	@XmlElement(name = "sku")
	List<String> bundledIn;

	public Map<String, Object> toMap() {
		Map<String, Object> document = new LinkedHashMap<>();
		document.put("sku", sku);
		document.put("productId", productId);
		document.put("name", name);
		document.put("type", type);
		document.put("startDate", startDate);
		document.put("activeUpdateDate", activeUpdateDate);
		document.put("regularPrice", regularPrice);
		document.put("salePrice", salePrice);
		document.put("onSale", onSale);
		document.put("priceUpdateDate", priceUpdateDate);
		document.put("digital", digital);
		document.put("productTemplate", productTemplate);
		document.put("categories", categories.stream().map(elem -> elem.name).collect(toList()));
		document.put("customerReviewCount", customerReviewCount);
		document.put("customerReviewAverage", customerReviewAverage);
		document.put("customerTopRated", customerTopRated);
		document.put("freeShipping", freeShipping);
		document.put("freeShippingEligible", freeShippingEligible);
		document.put("inStoreAvailability", inStoreAvailability);
		document.put("inStoreAvailabilityText", inStoreAvailabilityText);
		document.put("inStoreAvailabilityUpdateDate", inStoreAvailabilityUpdateDate);
		document.put("itemUpdateDate", itemUpdateDate);
		document.put("shippingCost", shippingCost);
		document.put("specialOrder", specialOrder);
		document.put("shortDescription", shortDescription);
		document.put("productClass", productClass);
		document.put("productSubclass", productSubclass);
		document.put("department", department);
		document.put("manufacturer", manufacturer);
		document.put("weight", weight);
		document.put("shippingWeight", shippingWeight);
		document.put("longDescription", longDescription);
		document.put("includedItemList", includedItemList);
		document.put("features", features);
		document.put("quantityLimit", quantityLimit);
		document.put("bundledIn", bundledIn);
		return document;
	}

	@XmlRootElement
	private static class ProductCategory {
		@XmlElement
		String name;
	}
}
