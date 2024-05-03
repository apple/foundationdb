/* https://github.com/jzaefferer/jquery-treeview/blob/master/jquery.treeview.edit.js */
/* License: MIT. */
(function($) {
	var CLASSES = $.treeview.classes;
	var proxied = $.fn.treeview;
	$.fn.treeview = function(settings) {
		settings = $.extend({}, settings);
		if (settings.add) {
			return this.trigger("add", [settings.add]);
		}
		if (settings.remove) {
			return this.trigger("remove", [settings.remove]);
		}
		return proxied.apply(this, arguments).bind("add", function(event, branches) {
			$(branches).prev()
				.removeClass(CLASSES.last)
				.removeClass(CLASSES.lastCollapsible)
				.removeClass(CLASSES.lastExpandable)
			.find(">.hitarea")
				.removeClass(CLASSES.lastCollapsibleHitarea)
				.removeClass(CLASSES.lastExpandableHitarea);
			$(branches).find("li").andSelf().prepareBranches(settings).applyClasses(settings, $(this).data("toggler"));
		}).bind("remove", function(event, branches) {
			var prev = $(branches).prev();
			var parent = $(branches).parent();
			$(branches).remove();
			prev.filter(":last-child").addClass(CLASSES.last)
				.filter("." + CLASSES.expandable).replaceClass(CLASSES.last, CLASSES.lastExpandable).end()
				.find(">.hitarea").replaceClass(CLASSES.expandableHitarea, CLASSES.lastExpandableHitarea).end()
				.filter("." + CLASSES.collapsible).replaceClass(CLASSES.last, CLASSES.lastCollapsible).end()
				.find(">.hitarea").replaceClass(CLASSES.collapsibleHitarea, CLASSES.lastCollapsibleHitarea);
			if (parent.is(":not(:has(>))") && parent[0] != this) {
				parent.parent().removeClass(CLASSES.collapsible).removeClass(CLASSES.expandable)
				parent.siblings(".hitarea").andSelf().remove();
			}
		});
	};
	
})(jQuery);
