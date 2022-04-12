import java.io.UnsupportedEncodingException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Tenant;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.KeyArrayResult;
import com.apple.foundationdb.TenantManagement;
import com.apple.foundationdb.async.AsyncUtil;
import static com.apple.foundationdb.async.AsyncUtil.collectRemaining;
import com.apple.foundationdb.async.CloseableAsyncIterator;

public class TenantTest {
	private FDB fdb;
	private Database db;
	CloseableAsyncIterator<KeyValue> tenants;

	public TenantTest() {
		try {
			fdb = FDB.selectAPIVersion(710);
			fdb.options().setTraceEnable(null);
			db = fdb.open();
///*
			Tuple t1 = Tuple.from("tenant");
			Tuple t2 = Tuple.from("tenant2");
			Tuple t3 = Tuple.from("tenant3");
//*/
/*
			byte[] t1 = Tuple.from("tenant").pack();
			byte[] t2 = Tuple.from("tenant2").pack();
			byte[] t3 = Tuple.from("tenant3").pack();
*/
			System.out.println(t1);
			System.out.println(t2);
			System.out.println(t3);

			TenantManagement.createTenant(db, t1).join();
			TenantManagement.createTenant(db, t2).join();
			TenantManagement.createTenant(db, t3).join();

			tenants = TenantManagement.listTenants(db, Tuple.from("a").pack(), Tuple.from("z").pack(), 100);

			try {
/*
				List<KeyValue> result = AsyncUtil.collectRemaining(tenants).join();
				System.out.println("Size: " + result.size());
				for(int i = 0; i < result.size(); i++) {
					System.out.println(i);
					KeyValue res = result.get(i);
					System.out.println(new String(res.getKey()));
					System.out.println(new String(res.getValue()));
				}
*/
//	/*
				while (tenants.hasNext()) {
					KeyValue res = tenants.next();
					System.out.println(new String(res.getKey()));
					System.out.println(new String(res.getValue()));
				}
//	*/
			}
			finally {
				tenants.close();
			}
			TenantManagement.deleteTenant(db, t1).join();
			TenantManagement.deleteTenant(db, t2).join();
			TenantManagement.deleteTenant(db, t3).join();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		db.close();
	}

	public static void main(String[] args) {
		new TenantTest().close();
	}
}

