package insightdatascience;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Insight data analysis challenge. 
 * Sample input :
 * ./input/test_1/order_products.csv ./input/test_1/products.csv ./output/test_1/report.csv
 * 
 * 
 * @author dharmendra
 *
 */
public class PurchaseAnalysisApp {
	public static DecimalFormat format = new DecimalFormat("0.00");

	public static void main(String[] args) {

		if (null == args || args.length != 3) {
			System.err.println(" please provide valid arguments in eg   orderFilePath productFilePath resultFilePath ");
		}

		String orderFilePath = args[0];
		String productFilePath = args[1];
		String resultReportFilePath = args[2];

		startAnalysisProcess(orderFilePath, productFilePath, resultReportFilePath);

	}

	/**
	 * Method to handle processing of data in files and create output result file.
	 * 
	 * @param orderFilePath
	 * @param productFilePath
	 * @param resultReportFilePath
	 */
	private static void startAnalysisProcess(String orderFilePath, String productFilePath,
			String resultReportFilePath) {
		Map<Integer, Integer> productDeptIdMap = readProductAndGetData(productFilePath);
		Map<Integer, Integer> deptTotalOrderMap = new HashMap<>();
		Map<Integer, Integer> deptFirstOrderMap = new HashMap<>();

		readOrderAndgetData(orderFilePath, deptTotalOrderMap, deptFirstOrderMap, productDeptIdMap);

//		System.out.println("productDeptIdMap" + productDeptIdMap + " \n deptTotalOrderMap " + deptTotalOrderMap
//				+ " \n deptFirstOrderMap " + deptFirstOrderMap);
		try (BufferedWriter responseFileWriter = new BufferedWriter(new FileWriter(resultReportFilePath))) {

			if (null != productDeptIdMap && productDeptIdMap.size() > 0) {

				responseFileWriter.write(new StringBuilder().append("department_id").append(",")
						.append("number_of_orders").append(",").append("number_of_first_orders").append(",")
						.append("percentage").append("\n").toString());

				productDeptIdMap.values().stream().distinct().sorted().forEach(deptId -> {
					Integer totalOder = deptTotalOrderMap.get(deptId);

					if (totalOder > 0) {

						int firstOrder = deptFirstOrderMap.get(deptId) != null
								? deptFirstOrderMap.get(deptId).intValue()
								: 0;

						double ratio = 0;
						if (firstOrder > 0) {
							ratio = (double) firstOrder / totalOder;
						}

						StringBuilder outputLine = new StringBuilder().append(deptId).append(",").append(totalOder)
								.append(",").append(firstOrder).append(",").append(format.format(ratio)).append("\n");

						try {

							responseFileWriter.write(outputLine.toString());

						} catch (IOException e) {
							System.err.println("Error writing to file, check logs ");
							e.printStackTrace();
						}
					}
				});

			}

		} catch (IOException e) {
			System.err.println("Excepton in writing to response file, check logs ");
			e.printStackTrace();
		}

	}

	/**
	 * Method to read Orders and get Data in Map. 
	 * Sample data in file 2,33120,1,1
	 * 
	 * @param orderFilePath
	 * @param deptTotalOrderMap
	 * @param deptFirstOrderMap
	 * @param deptProductIdMap
	 */
	private static void readOrderAndgetData(String orderFilePath, Map<Integer, Integer> deptTotalOrderMap,
			Map<Integer, Integer> deptFirstOrderMap, Map<Integer, Integer> deptProductIdMap) {
		try (Stream<String> lineStream = Files.lines(Paths.get(orderFilePath))) {

			lineStream.skip(1).forEach(x -> {
				String[] values = x.split(",");
				int productId = (null != values[1]) ? Integer.parseInt(values[1]) : 0;
				int firstOrder = (null != values[2]) ? Integer.parseInt(values[3]) : 0;

				if (productId > 0) {
					Integer departMentId = deptProductIdMap.get(productId);
					if (null != departMentId && departMentId > 0) {
						if (firstOrder == 0) {
							deptFirstOrderMap.merge(departMentId, 1, Integer::sum);
						}
						deptTotalOrderMap.merge(departMentId, 1, Integer::sum);
					}
				}
			});

		} catch (IOException e) {
			System.err.println("error in reading order File , check orderFilePath");
			e.printStackTrace();
		}

	}

	/**
	 * Method to read product files and get Data in map. 
	 * Sample data in file 49644,Feta Crumbles,2,16
	 * 
	 * @param productFilePath
	 * @return
	 */
	private static Map<Integer, Integer> readProductAndGetData(String productFilePath) {
		Map<Integer, Integer> deptProductIdMap = new HashMap<>();

		try (Stream<String> lineStream = Files.lines(Paths.get(productFilePath))) {

			lineStream.skip(1).forEach(x -> {
				String deptId = x.split(",")[3];
				String productId = x.split(",")[0];
				if (null != deptId && null != productId) {
					deptProductIdMap.put(Integer.parseInt(productId.trim()), Integer.parseInt(deptId.trim()));
				}
			});

		} catch (IOException e) {
			System.err.println("error in reading product File , check productFilePath");
			e.printStackTrace();
		}

		return deptProductIdMap;
	}
}
